import { PurchaseTicketDto } from '@app/common';
import { DatabaseService, events, tickets } from '@app/database';
import { KAFKA_SERVICE, KAFKA_TOPICS } from '@app/kafka';
import {
  BadRequestException,
  ForbiddenException,
  Inject,
  Injectable,
  NotFoundException,
  OnModuleInit,
} from '@nestjs/common';
import { ClientKafka } from '@nestjs/microservices';
import { randomBytes } from 'crypto';
import { and, eq, sql } from 'drizzle-orm';

@Injectable()
export class TicketsServiceService implements OnModuleInit {
  constructor(
    @Inject(KAFKA_SERVICE) private readonly kafkClient: ClientKafka,
    private readonly dbService: DatabaseService,
  ) { }

  /**
   * 📌 Modul işə düşəndə:
   * Kafka broker ilə əlaqə qurulur.
   */
  async onModuleInit() {
    await this.kafkClient.connect();
  }

  /**
   * 🎲 Bilet üçün unikal kod istehsal edir (Məs: '3F8A1B2C4D5E').
   * Bu kod konsert qapısında skan ediləcək və ya yoxlanacaq koddur.
   */
  private generateTicketCode(): string {
    return randomBytes(6).toString('hex').toUpperCase();
  }

  /**
   * 📌 1. BİLET SATIN ALMAQ (Purchase Ticket - Ən Kritik Biznes Məntiqi)
   * ----------------------------------------------------------------------
   * @param purchaseDto - { eventId, quantity }
   * @param userId - Bileti alan müştərinin ID-si (Gateway-dən gələn x-user-id)
   *
   * 🔄 Proses:
   * 1. Tədbir yoxlanılır: Tədbir mövcuddurmu?
   * 2. Status yoxlanılır: Yalnız 'PUBLISHED' (satışda olan) tədbirə bilet almaq olar!
   * 3. OVERBOOKING-İN QARŞISINI ALMAQ (Qalan yerlərin hesablanması):
   *    - Baza üzərində SUM(quantity) hesablanır: Bu tədbir üçün indiyə qədər neçə bilet satılıb?
   *    - Qalan yer = event.capacity - satılmış yerlər.
   *    - Əgər istifadəçinin istədiyi say qalan yerdən çoxdursa, dərhal '400 Bad Request' atılır.
   * 4. Bilet ilkin olaraq 'PENDING' statusu ilə bazaya yazılır.
   * 5. KAFKA EMIT: 'ticket.purchased' hadisəsi atılır!
   *    💡 Niyə Kafka?: Çünki Ödəniş servisi bu xəbəri alıb ödənişi təsdiqləyəcək,
   *    Mail servisi isə müştəriyə biletin PDF-ini və QR-kodunu email atacaq!
   */
  async purchase(purchaseDto: PurchaseTicketDto, userId: string) {
    const { eventId, quantity } = purchaseDto;

    // 1. Tədbirin mövcudluğunu yoxla
    const [event] = await this.dbService.db
      .select()
      .from(events)
      .where(eq(events.id, eventId))
      .limit(1);

    if (!event) {
      throw new NotFoundException('Event not found');
    }

    if (event.status !== 'PUBLISHED') {
      throw new BadRequestException('Event is not published');
    }

    // 2. Overbooking (tutumu aşmaq) yoxlanışı: Satılmış yerləri topla
    const soldTickets = await this.dbService.db
      .select({ total: sql<number>`COALESCE(SUM(${tickets.quantity}), 0)` })
      .from(tickets)
      .where(
        and(eq(tickets.eventId, eventId), eq(tickets.status, 'CONFIRMED')),
      );

    const currentSold = Number(soldTickets[0]?.total || 0);
    const remaining = event.capacity - currentSold;

    if (quantity > remaining) {
      throw new BadRequestException(`Only ${remaining} tickets remaining`);
    }

    const totalPrice = event.price * quantity;

    // 3. Bileti 'PENDING' statusu ilə bazaya yaz
    const [ticket] = await this.dbService.db
      .insert(tickets)
      .values({
        eventId,
        userId,
        quantity,
        totalPrice,
        ticketCode: this.generateTicketCode(),
        status: 'PENDING',
      })
      .returning();

    // 📢 4. Kafka-ya xəbər ver: Ödəniş və Email servisləri işə başlasın
    this.kafkClient.emit(KAFKA_TOPICS.TICKET_PURCHASED, {
      ticketId: ticket.id,
      eventId: ticket.eventId,
      userId: ticket.userId,
      quantity: ticket.quantity,
      totalPrice: ticket.totalPrice,
      ticketCode: ticket.ticketCode,
      timestamp: new Date().toISOString(),
    });

    return {
      message: 'Ticket purchased successfully',
      ticket: {
        id: ticket.id,
        ticketCode: ticket.ticketCode,
        eventTitle: event.title,
        quantity: ticket.quantity,
        totalPrice: ticket.totalPrice,
        status: ticket.status,
        purchasedAt: ticket.purchasedAt,
      },
    };
  }

  /**
   * 📌 2. MÜŞTƏRİNİN ŞƏXSİ BİLETLƏRİ (Find My Tickets)
   * ----------------------------------------------------------------------
   * @param userId - Giriş etmiş istifadəçinin ID-si
   *
   * 🔄 Proses:
   * - 'tickets' cədvəlini 'events' cədvəli ilə INNER JOIN edir.
   * - Müştəriyə təkcə bilet kodunu yox, həm də konsertin adı, tarixi və məkanını birlikdə qaytarır.
   */
  async findMyTicket(userId: string) {
    const userTickets = await this.dbService.db
      .select({
        id: tickets.id,
        ticketCode: tickets.ticketCode,
        quantity: tickets.quantity,
        totalPrice: tickets.totalPrice,
        status: tickets.status,
        purchasedAt: tickets.purchasedAt,
        checkedInAt: tickets.checkedInAt,
        eventId: events.id,
        eventTitle: events.title,
        eventDate: events.date,
        eventLocation: events.location,
      })
      .from(tickets)
      .innerJoin(events, eq(tickets.eventId, events.id))
      .where(eq(tickets.userId, userId));

    return userTickets;
  }

  /**
   * 📌 3. TƏK BİR BİLETİN DETALLARI (Find One Ticket)
   * ----------------------------------------------------------------------
   * @param id - Biletin ID-si
   * @param userId - Giriş etmiş istifadəçinin ID-si
   *
   * 🔄 Proses:
   * 1. Bilet bazadan tapılır.
   * 2. Təhlükəsizlik yoxlanışı: Kimsə başqa birinin bilet ID-sini yazıb baxa bilməz!
   *    Yalnız biletin sahibi öz biletini aça bilər.
   */
  async findOne(id: string, userId: string) {
    const [ticket] = await this.dbService.db
      .select({
        id: tickets.id,
        ticketCode: tickets.ticketCode,
        quantity: tickets.quantity,
        totalPrice: tickets.totalPrice,
        status: tickets.status,
        purchasedAt: tickets.purchasedAt,
        checkedInAt: tickets.checkedInAt,
        userId: tickets.userId,
        eventId: events.id,
        eventTitle: events.title,
        eventDate: events.date,
        eventLocation: events.location,
      })
      .from(tickets)
      .innerJoin(events, eq(tickets.eventId, events.id))
      .where(and(eq(tickets.id, id), eq(tickets.userId, userId)))
      .limit(1);

    if (!ticket) {
      throw new NotFoundException('Ticket not found');
    }

    if (ticket.userId !== userId) {
      throw new ForbiddenException('Ticket does not belong to user');
    }

    return ticket;
  }

  /**
   * 📌 4. BİLETİ LƏĞV ETMƏK / İADƏ ETMƏK (Cancel Ticket)
   * ----------------------------------------------------------------------
   * @param id - Biletin ID-si
   * @param userId - Ləğv etmək istəyən istifadəçi
   *
   * 🔄 Proses:
   * 1. Biletin sahibi olub-olmaması yoxlanılır.
   * 2. Artıq ləğv edilmiş və ya artıq konsert qapısından içəri girmiş ('CHECKED_IN')
   *    biletin ləğvinə icazə verilmir!
   * 3. Biletin statusu 'CANCELLED' edilir.
   * 4. KAFKA EMIT: 'ticket.cancelled' hadisəsi atılır ki, müştəriyə pul iadəsi (refund) edilsin.
   */
  async cancel(id: string, userId: string) {
    const [ticket] = await this.dbService.db
      .select()
      .from(tickets)
      .where(eq(tickets.id, id))
      .limit(1);

    if (!ticket) {
      throw new NotFoundException('Ticket not found');
    }

    if (ticket.userId !== userId) {
      throw new ForbiddenException('Ticket does not belong to user');
    }

    if (ticket.status === 'CANCELLED') {
      throw new BadRequestException('Ticket is already cancelled');
    }

    if (ticket.status === 'CHECKED_IN') {
      throw new BadRequestException('Ticket is already checked in');
    }

    const [cancelled] = await this.dbService.db
      .update(tickets)
      .set({ status: 'CANCELLED', updatedAt: new Date() })
      .where(eq(tickets.id, id))
      .returning();

    // 📢 Kafka-ya hadisə: Bilet ləğv edildi, pulu geri qaytarın
    this.kafkClient.emit(KAFKA_TOPICS.TICKET_CANCELLED, {
      ticketId: cancelled.id,
      eventId: cancelled.eventId,
      userId: cancelled.userId,
      timestamp: new Date().toISOString(),
    });

    return { message: 'Ticket cancelled successfully' };
  }

  /**
   * 📌 5. KONSERT QAPISINDA BİLETLƏ DAXİL OLMAQ (Check-In)
   * ----------------------------------------------------------------------
   * @param ticketCode - Biletin unikal kodu (məs: '3F8A1B2C4D5E')
   * @param organizerId - Qapıda skan edən təşkilatçının ID-si
   *
   * 🔄 Proses (Real Həyat):
   * Tamaşaçı konsertin qapısına gəlir və telefonundakı bilet kodunu göstərir.
   * Qapıdakı nəzarətçi (təşkilatçı) bu kodu sistemə daxil edir:
   * 1. Bilet tapılır.
   * 2. Təhlükəsizlik: Skan edən adam həqiqətən bu konsertin təşkilatçısıdırmı? (Başqa konsertin biletini qəbul edə bilməz).
   * 3. Saxtakarlıq qoruması: Bu biletlə artıq içəri daxil olunubmu ('CHECKED_IN') və ya ləğv olunubmu?
   * 4. Hər şey qaydasındadırsa, biletin statusu 'CHECKED_IN' edilir və içəri buraxılır!
   * 5. KAFKA EMIT: 'ticket.checked_in' hadisəsi atılır (turniket statistika hesabatı üçün).
   */
  async checkIn(ticketCode: string, organizerId: string) {
    const [ticket] = await this.dbService.db
      .select({
        id: tickets.id,
        status: tickets.status,
        eventId: events.id,
        quantity: tickets.quantity,
      })
      .from(tickets)
      .where(eq(tickets.ticketCode, ticketCode))
      .limit(1);

    if (!ticket) {
      throw new NotFoundException('Ticket not found');
    }

    const [event] = await this.dbService.db
      .select()
      .from(events)
      .where(eq(events.id, ticket.eventId))
      .limit(1);

    // Qapıdakı adam bu tədbirin təşkilatçısı olmalıdır
    if (event.organizerId !== organizerId) {
      throw new ForbiddenException(
        'You are not authorized to check in this ticket',
      );
    }

    if (ticket.status === 'CHECKED_IN') {
      throw new BadRequestException('Ticket is already checked in');
    }

    if (ticket.status === 'CANCELLED') {
      throw new BadRequestException('Ticket is already cancelled');
    }

    const [checkedIn] = await this.dbService.db
      .update(tickets)
      .set({ status: 'CHECKED_IN', checkedInAt: new Date(), updatedAt: new Date(), })
      .where(eq(tickets.id, ticket.id))
      .returning();

    // 📢 Kafka-ya hadisə: Bilet uğurla qapıda təsdiqləndi
    this.kafkClient.emit(KAFKA_TOPICS.TICKET_CHECKED_IN, {
      ticketId: checkedIn.id,
      eventId: checkedIn.eventId,
      ticketCode: checkedIn.ticketCode,
      timestamp: new Date().toISOString(),
    });

    return {
      message: 'Ticket checked in successfully',
      ticket: {
        id: checkedIn.id,
        ticketCode: checkedIn.ticketCode,
        quantity: checkedIn.quantity,
        status: checkedIn.status,
        checkedInAt: checkedIn.checkedInAt,
      },
    };
  }

  /**
   * 📌 6. TƏDBİRƏ SATILMIŞ BÜTÜN BİLETLƏRİN SİYAHISI (Event Tickets List)
   * ----------------------------------------------------------------------
   * @param eventId - Tədbirin ID-si
   * @param organizerId - Sorğu göndərən təşkilatçı
   *
   * 🔄 Proses:
   * - Yalnız tədbirin sahibi olan təşkilatçı öz tədbirinə satılmış bütün biletlərin
   *   və alıcıların siyahısını görə bilər.
   */
  async findEventTickst(eventId: string, organizerId: string) {
    const [event] = await this.dbService.db.select().from(events).where(eq(events.id, eventId)).limit(1);

    if (!event) throw new NotFoundException('Event not found');


    if (event.organizerId !== organizerId) {
      throw new ForbiddenException('You are not authorized to check in this ticket',);
    }

    return this.dbService.db.select().from(tickets).where(eq(tickets.eventId, eventId));
  }
}
