import { CreateEventDto, UpdateEventDto } from '@app/common';
import { DatabaseService, events } from '@app/database';
import { KAFKA_SERVICE, KAFKA_TOPICS } from '@app/kafka';
import {
  ForbiddenException,
  Inject,
  Injectable,
  NotFoundException,
  OnModuleInit,
} from '@nestjs/common';
import { ClientKafka } from '@nestjs/microservices';
import { eq } from 'drizzle-orm';

@Injectable()
export class EventsServiceService implements OnModuleInit {
  constructor(
    @Inject(KAFKA_SERVICE) private readonly kafkClient: ClientKafka,
    private readonly dbService: DatabaseService,
  ) { }

  /**
   * 📌 Modul işə düşəndə (Startup):
   * Kafka broker-ə (localhost:9093) avtomatik əlaqə qurulur.
   */
  async onModuleInit() {
    await this.kafkClient.connect();
  }

  /**
   * 📌 1. YENİ TƏDBİR YARATMAQ (Create Event)
   * -------------------------------------------------------------
   * @param createEventDto - Tədbirin detalları (ad, qiymət, tutum, tarix və s.)
   * @param organizerId - Tədbiri yaradan təşkilatçının ID-si (Gateway-dən gələn x-user-id)
   *
   * 🔄 Proses:
   * 1. Məlumatlar Drizzle ORM ilə PostgreSQL-in 'events' cədvəlinə yazılır.
   * 2. Yeni yaradılan tədbirin ilkin statusu bazada default olaraq 'DRAFT' (Qaralama) olur.
   * 3. KAFKA EMIT: 'event.created' topic-inə bildiriş atılır ki, digər servislər
   *    (məsələn: Bilet və ya Analitika servisi) yeni tədbirin yaradıldığını bilsin.
   */
  async create(createEventDto: CreateEventDto, organizerId: string) {
    const [event] = await this.dbService.db
      .insert(events)
      .values({
        ...createEventDto,
        date: new Date(createEventDto.date),
        price: createEventDto.price || 0,
        organizerId,
      })
      .returning();

    // 📢 Kafka-ya hadisə atılır: Digər servislər xəbərdar olsun
    this.kafkClient.emit(KAFKA_TOPICS.EVENT_CREATED, {
      eventId: event.id,
      organizerId: event.organizerId,
      title: event.title,
      timestamp: new Date().toISOString(),
    });

    return event;
  }

  /**
   * 📌 2. BÜTÜN DƏRC OLUNMUŞ TƏDBİRLƏRİ GƏTİRMƏK (Find All - Ana Səhifə)
   * -------------------------------------------------------------
   * 🔄 Proses:
   * - Sayta daxil olan hər kəsə (ictimai) yalnız rəsmi satışa çıxmış 'PUBLISHED'
   *   tədbirlər göstərilir. 'DRAFT' (hələ hazır olmayan) və ya 'CANCELLED' (ləğv edilmiş)
   *   tədbirlər bu siyahıya düşmür.
   * - Sadə oxuma (SELECT) əməliyyatı olduğu üçün KAFKA istifadə edilmir.
   */
  async findAll() {
    return this.dbService.db
      .select()
      .from(events)
      .where(eq(events.status, 'PUBLISHED'));
  }

  /**
   * 📌 3. TƏŞKİLATÇININ ŞƏXSİ TƏDBİRLƏRİ (Find My Events)
   * -------------------------------------------------------------
   * @param organizerId - Hazırda daxil olmuş təşkilatçının ID-si
   *
   * 🔄 Proses:
   * - Təşkilatçının şəxsi kabinetində yalnız özünə aid olan tədbirləri gətirir.
   * - Burada status fərqi yoxdur: Həm DRAFT, həm PUBLISHED, həm də CANCELLED tədbirlərini görür.
   */
  async findMyEvent(organizerId: string) {
    return this.dbService.db
      .select()
      .from(events)
      .where(eq(events.organizerId, organizerId));
  }

  /**
   * 📌 4. TƏK BİR TƏDBİRİN DETALLARI (Find One)
   * -------------------------------------------------------------
   * @param id - Tədbirin unikal UUID-si
   *
   * 🔄 Proses:
   * - ID-yə əsasən bazadan axtarış edilir.
   * - Əgər tapılmazsa, dərhal '404 Not Found' xətası atılır.
   */
  async findOne(id: string) {
    const [event] = await this.dbService.db
      .select()
      .from(events)
      .where(eq(events.id, id))
      .limit(1);

    if (!event) {
      throw new NotFoundException('Event not found');
    }

    return event;
  }

  /**
   * 📌 5. TƏDBİRİ YENİLƏMƏK / REDAKTƏ ETMƏK (Update Event)
   * -------------------------------------------------------------
   * @param id - Yenilənəcək tədbirin ID-si
   * @param updateEventDto - Dəyişdiriləcək sahələr (qiymət, məkan və s.)
   * @param userId - Sorğunu göndərən istifadəçinin ID-si
   * @param userRole - İstifadəçinin rolu (məsələn: 'ADMIN' və ya 'USER')
   *
   * 🔄 Proses:
   * 1. Əvvəlcə tədbir bazadan tapılır.
   * 2. İcazə yoxlanılır: Bu tədbiri yalnız onu YARADAN ŞƏXS və ya sistem ADMIN-i dəyişə bilər!
   *    Əks halda '403 Forbidden' xətası verilir.
   * 3. Dəyişikliklər bazada saxlanılır və 'updatedAt' tarixi yenilənir.
   * 4. KAFKA EMIT: 'event.updated' topic-inə hadisə atılır (hansı sahələrin dəyişdiyi bildirilir).
   */
  async update(id: string, updateEventDto: UpdateEventDto, userId: string, userRole: string,) {
    const event = await this.findOne(id);

    // 🔒 Təhlükəsizlik: Başqasının tədbirini redaktə etməyə icazə verilmir
    if (event.organizerId !== userId && userRole !== 'ADMIN') {
      throw new ForbiddenException(
        'You are not authorized to update this event',
      );
    }

    const updatedData: Record<string, unknown> = { ...updateEventDto };
    if (updateEventDto.date) {
      updatedData.date = new Date(updateEventDto.date);
    }
    updatedData.updatedAt = new Date();

    const [updated] = await this.dbService.db
      .update(events)
      .set(updatedData)
      .where(eq(events.id, id))
      .returning();

    // 📢 Kafka-ya hadisə atılır: Tədbir məlumatları yeniləndi
    this.kafkClient.emit(KAFKA_TOPICS.EVENT_UPDATED, {
      eventId: updated.id,
      changes: Object.keys(updateEventDto),
      timestamp: new Date().toISOString(),
    });

    return updated;
  }

  /**
   * 📌 6. TƏDBİRİ SATIŞA AÇMAQ / DƏRC ETMƏK (Publish Event)
   * -------------------------------------------------------------
   * @param id - Dərc ediləcək tədbirin ID-si
   * @param userId - Sorğunu göndərən şəxsin ID-si
   * @param userRole - Rolu (Admin yoxlanışı üçün)
   *
   * 🔄 Proses:
   * 1. İcazə yoxlanılır (yalnız sahibi və ya Admin dərc edə bilər).
   * 2. Tədbirin statusu 'PUBLISHED' edilir. Artıq tədbir ana səhifədə (findAll) görünür
   *    və bilet satışına hazır olur.
   */
  async publish(id: string, userId: string, userRole: string) {
    const event = await this.findOne(id);

    if (event.organizerId !== userId && userRole !== 'ADMIN') {
      throw new ForbiddenException(
        'You are not authorized to publish this event',
      );
    }

    const [published] = await this.dbService.db
      .update(events)
      .set({ status: 'PUBLISHED', updatedAt: new Date() })
      .where(eq(events.id, id))
      .returning();

    return published;
  }

  /**
   * 📌 7. TƏDBİRİ LƏĞV ETMƏK (Cancel Event)
   * -------------------------------------------------------------
   * @param id - Ləğv ediləcək tədbirin ID-si
   * @param userId - Sorğunu göndərən şəxsin ID-si
   * @param userRole - Rolu
   *
   * 🔄 Proses:
   * 1. İcazə yoxlanılır.
   * 2. Tədbirin statusu 'CANCELLED' edilir (artıq yeni bilet alışı dayandırılır).
   * 3. KAFKA EMIT: 'event.cancelled' topic-inə kritik hadisə atılır!
   *    💡 Niyə Kafka?: Çünki bu hadisəni eşidən Bilet və Ödəniş servisləri
   *    artıq bilet almış insanlara pullarını geri qaytarmalı (refund) və bildiriş atmalıdır!
   */
  async cancel(id: string, userId: string, userRole: string) {
    const event = await this.findOne(id);

    if (event.organizerId !== userId && userRole !== 'ADMIN') {
      throw new ForbiddenException(
        'You are not authorized to cancel this event',
      );
    }

    const [cancelled] = await this.dbService.db
      .update(events)
      .set({ status: 'CANCELLED', updatedAt: new Date() })
      .where(eq(events.id, id))
      .returning();

    // 📢 Kafka-ya hadisə atılır: Digər servislər pul iadəsi (refund) prosesini başlatsın
    this.kafkClient.emit(KAFKA_TOPICS.EVENT_CANCELLED, {
      eventId: cancelled.id,
      organizerId: cancelled.organizerId,
      timestamp: new Date().toISOString(),
    });

    return cancelled;
  }
}
