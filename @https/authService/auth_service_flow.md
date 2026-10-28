# 🏛️ AuthService, PostgreSQL və Apache Kafka Necə İşləyir?

Bu sənəd layihənizdə qaldırdığınız servislərin (`api-gateway`, `auth-service`, `postgres`, `kafka`, `kafka-ui`, `zookeeper`) bir-biri ilə necə əlaqə qurduğunu və `register`, `login`, `getProfile` əməliyyatlarında nə baş verdiyini izah edir.

---

## 📊 1. Sistem Arxitekturasının Ümumi Mənzərəsi

```text
[Müştəri: Postman / Brauzer]
             │
             ▼
     ┌────────────────┐
     │  API Gateway   │  (Port 3000) - Bütün xarici sorğuları qəbul edən tək qapı
     └───────┬────────┘
             │
             ▼
     ┌────────────────┐
     │  Auth Service  │  (Port 3001) - Qeydiyyat, Giriş və JWT idarəçiliyi
     └───────┬────────┘
             │
    ┌────────┴───────────────────┐
    ▼                            ▼
┌──────────────────┐    ┌─────────────────────────────────┐
│ PostgreSQL (5432)│    │      Apache Kafka (9093)        │
│   (Drizzle ORM)  │    │  (Event-Driven Mesaj Brokeri)   │
└──────────────────┘    └────────────────┬────────────────┘
  - users cədvəli                        │
  - Şifrə hash-ləri                      ▼
                        ┌─────────────────────────────────┐
                        │    Kafka Topics (Poçt Qutusu)   │
                        │    - user.registered            │
                        │    - user.login                 │
                        └─────────────────────────────────┘
                                         │
                                         ▼ (Gələcəkdə)
                        ┌─────────────────────────────────┐
                        │ Email / Notification Servisi    │
                        │ (Xoşgəldin emaili göndərəcək)   │
                        └─────────────────────────────────┘
```

---

## 🔄 2. Üç Əsas Əməliyyatın Dərin Analizi

### 1️⃣ Qeydiyyat Prosesi (`POST /register`)
* **Nə baş verir?**:
  1. İstifadəçi `email`, `password`, `name` göndərir.
  2. Drizzle ORM ilə PostgreSQL-in `users` cədvəlində yoxlanılır (təkrar email varsa `409 Conflict` atılır).
  3. Şifrə `bcrypt.hash()` ilə 10 duzlama dərəcəsi ilə təhlükəsiz şifrələnir.
  4. İstifadəçi PostgreSQL bazasına yazılır.
  5. **KAFKA EMIT:** `kafkaClient.emit('user.registered', { userId, email, name, timestamp })` çağırılır.
  6. İstifadəçiyə dərhal cavab qayıdır: `{ message: "User registered successfully", userId }`.
* **Kafka burda nə edir?**:
  * Hadisəni (Event) Kafka-nın `user.registered` topic-inə atır.
  * `AuthService` heç kimi gözləmir. Gələcəkdə yazacağımız `Notification Service` bu topic-dən xəbəri tutub istifadəçiyə "Xoş gəldiniz" emaili göndərəcək.

---

### 2️⃣ Giriş Prosesi (`POST /login`)
* **Nə baş verir?**:
  1. İstifadəçi `email` və `password` göndərir.
  2. Baza yoxlanılır, tapılmazsa və ya `bcrypt.compare()` səhv olarsa `401 Unauthorized` qayıdır.
  3. Düzgündürsə, `jwtService.sign(...)` ilə rəqəmsal imzalı **JWT Token** hazırlanır.
  4. **KAFKA EMIT:** `kafkaClient.emit('user.login', { userId, timestamp })` çağırılır.
  5. İstifadəçiyə cavab verilir: `{ access_token, user: { id, email, name, role } }`.
* **Kafka burda nə edir?**:
  * Kimin nə vaxt daxil olduğunu izləyir (Audit, Təhlükəsizlik və Statistika üçün).

---

### 3️⃣ Profil Oxunuşu (`GET /profile`)
* **Nə baş verir?**:
  1. İstifadəçi `Authorization: Bearer <TOKEN>` header-i ilə sorğu göndərir.
  2. `PassportStrategy` (`jwt.strategy.ts`) tokenin imzasını yoxlayır, içindən `userId`-ni çıxarır.
  3. Drizzle ORM ilə həmin istifadəçinin məlumatları bazadan oxunur və ekrana verilir.
* **KAFKA BURADA ROL OYNAYIRMI?**:
  * **XEYR! Qətiyyən oynamır.**
  * Çünki profilə baxmaq bir **oxuma (Query)** əməliyyatıdır. Burada heç bir sistem hadisəsi baş vermir. Kafka yalnız **hadisələr (Event)** baş verəndə istifadə olunur!

---

## ⚡ 3. Kafka Bizə Nə Qazandırır? (Monolit vs Event-Driven)

| Əlamət | Köhnə Yol (Kafkasız / Monolit) | Yeni Yol (Kafka İlə Event-Driven) |
| :--- | :--- | :--- |
| **Sürət** | Qeydiyyat zamanı emailin getməsini 2 saniyə gözləyirik. | Qeydiyyat 50 millisaniyədə bitir, email arxa fonda asinxron gedir. |
| **Xəta Dayanıqlığı** | Email servisi xarab olanda bütün qeydiyyat çökür. | Email servisi sönülü olsa belə qeydiyyat işləyir. Sabah email servisi qalxanda növbədəki bütün mailləri göndərəcək. |
| **Genişlənmə** | Yeni servis əlavə edəndə köhnə kodu söküb dəyişməliyik. | Sadəcə yeni servisi Kafka-ya bağlayırıq; mövcud `AuthService`-ə toxunmuruq. |

---

## 🧪 4. Canlı Test Təlimatı

### Addım 1: İstifadəçi Qeydiyyatı
```bash
curl -X POST http://localhost:3001/register \
  -H "Content-Type: application/json" \
  -d '{"email": "ali@example.com", "password": "password123", "name": "Ali Valiyev"}'
```

### Addım 2: Canlı Kafka-da İzləmə
Brauzerdə **`http://localhost:8080`** (Kafka UI) ünvanına daxil olun.
* Sol menyudan **Topics** bölməsinə keçin.
* **`user.registered`** topic-inə klikləyin və **Messages** bölməsində göndərdiyiniz istifadəçi məlumatını canlı görün!

### Addım 3: Giriş Edib Token Almaq
```bash
curl -X POST http://localhost:3001/login \
  -H "Content-Type: application/json" \
  -d '{"email": "ali@example.com", "password": "password123"}'
```
Geri qayıdan `access_token`-i kopyalayın.

### Addım 4: Profilə Baxmaq
```bash
curl -X GET http://localhost:3001/profile \
  -H "Authorization: Bearer <KOPYALANAN_TOKEN>"
```
