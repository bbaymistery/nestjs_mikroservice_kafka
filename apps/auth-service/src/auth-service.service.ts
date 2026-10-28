import { DatabaseService, users } from '@app/database';
import { KAFKA_SERVICE, KAFKA_TOPICS } from '@app/kafka';
import { ConflictException, Inject, Injectable, OnModuleInit, UnauthorizedException, } from '@nestjs/common';
import { JwtService } from '@nestjs/jwt';
import { ClientKafka } from '@nestjs/microservices';
import { eq } from 'drizzle-orm';
import * as bcrypt from 'bcrypt';

@Injectable()
export class AuthServiceService implements OnModuleInit {
  constructor(
    @Inject(KAFKA_SERVICE) private readonly kafkClient: ClientKafka,
    private readonly dbService: DatabaseService,
    private readonly jwtService: JwtService,
  ) { }

  async onModuleInit() {
    //connect to kafka when module initializies
    await this.kafkClient.connect();

  }


  /**
   * 📌 İSTİFADƏÇİ QEYDİYYAT PROSESİ:
   * 1. Controller-dən (POST /register) gələn DTO məlumatları (email, password, name) bura çatır.
   * 2. Drizzle ORM vasitəsilə PostgreSQL bazası yoxlanılır (email artıq varsa xəta atılır).
   * 3. Şifrə bcrypt ilə hash-lənir (təhlükəsizlik üçün bazaya açıq şifrə yazılmır).
   * 4. Yeni istifadəçi PostgreSQL-in 'users' cədvəlinə yazılır və geri qaytarılır.
   * 5. KAFKA EMIT: Qeydiyyat hadisəsi (Event) Kafka Broker-ə göndərilir.
   */
  async register(email: string, password: string, name: string) {
    // 1. İstifadəçinin bazada olub-olmaması yoxlanılır
    const exitingUser = await this.dbService.db
      .select()
      .from(users)
      .where(eq(users.email, email))
      .limit(1);

    if (exitingUser.length > 0) {
      throw new ConflictException('User already exists');
    }

    // 2. Şifrə hash-lənir
    const hashedPassword = await bcrypt.hash(password, 10);

    // 3. İstifadəçi bazaya yazılır və .returning() ilə yaradılmış istifadəçi obyekti alınır
    const [user] = await this.dbService.db
      .insert(users)
      .values({ email, password: hashedPassword, name })
      .returning();

    // 4. KAFKA-YA HADİSƏ GÖNDƏRİLMƏSİ (HARA VƏ NİYƏ?):
    // - HARA GÖNDƏRİRİK?: Kafka Broker-in daxilindəki "user.registered" adlı Topic-ə (poçt qutusuna).
    // - NƏ ÜÇÜN?: AuthService istifadəçini qeydiyyatdan keçirdi. İndi bu xəbəri Kafka-ya atırıq ki,
    //   digər müstəqil servislər (məs: Email/Notification Service) bu Topic-dən xəbəri tutub
    //   istifadəçiyə xoşgəldin emaili göndərsin. AuthService onların işini gözləmədən dərhal cavab qaytarır.
    this.kafkClient.emit(KAFKA_TOPICS.USER_REGISTERED, {
      userId: user.id,
      email: user.email,
      name: user.name,
      timestamp: new Date().toISOString(),
    });

    return { message: 'User registered successfully', userId: user.id };
  }

  async login(email: string, password: string) {
    const [user] = await this.dbService.db
      .select()
      .from(users)
      .where(eq(users.email, email))
      .limit(1);

    if (!user || !(await bcrypt.compare(password, user.password))) {
      throw new UnauthorizedException('Invalid credentials');
    }

    const token = this.jwtService.sign({ sub: user.id, email: user.email });

    this.kafkClient.emit(KAFKA_TOPICS.USER_LOGIN, {
      userId: user.id,
      timestamp: new Date().toISOString(),
    });

    return {
      access_token: token,
      user: {
        id: user.id,
        email: user.email,
        name: user.name,
        role: user.role,
      },
    };
  }

  async getProfile(userId: string) {
    const [user] = await this.dbService.db
      .select({
        id: users.id,
        email: users.email,
        name: users.name,
        role: users.role,
      })
      .from(users)
      .where(eq(users.id, userId))
      .limit(1);

    if (!user) {
      throw new UnauthorizedException('User not found');
    }

    return user;
  }
  // getHello(): string {
  //   return 'Hello World!';
  // }

  // async simulateUserRegistration(email: string) {
  //   //publish event to kafka 
  //   this.kafkaClient.emit(KAFKA_TOPICS.USER_REGISTERED, {
  //     email,
  //     timestamp: new Date().toISOString()
  //   });
  //   return `Event for user ${email} has been published to Kafka`;
  // }
}
