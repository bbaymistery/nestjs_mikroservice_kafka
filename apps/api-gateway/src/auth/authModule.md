# 🛡️ API Gateway - Auth Modülü (Reverse Proxy)

Bu modül **API Gateway (Port 3000)** daxilində yerləşir və xarici dünya (Frontend, Mobil tətbiq, Postman) ilə arxa plandakı **Auth Microservice (Port 3001)** arasında **Təhlükəsizlik Qapısı və Yönləndirici (Reverse Proxy)** rolunu oynayır.

---

## ❓ Əsas Sual: "Bizim zatən `auth-service` mikroservisimiz var, bəs Gateway-də niyə yenidən `auth` modulu yazdıq?"

Mikroservis arxitekturasında **heç vaxt daxili servislər (Auth, Events, Tickets) birbaşa internetə açılmır**.

Əgər bu modul Gateway-də olmasaydı:
1. **Frontend çaş-baş qalardı:** Mobil və veb tətbiq hər servis üçün ayrı port (Port 3001, Port 3002, Port 3003...) yadda saxlamalı idi.
2. **Təhlükəsizlik sıfır olardı:** Kimsə birbaşa daxili mikroservislərə hücum (Brute-force / DDoS) edə bilərdi.

### 🌟 API Gateway Auth Modulunun Qazandırdıqları:
1. **Tək Giriş Qapısı (Single Entry Point):** Xarici dünya yalnız tək ünvanı tanıyır: `http://localhost:3000`.
2. **DDoS və Brute-Force Qoruması (Rate Limiting):**
   * `@Throttle({ limit: 3, ttl: 60000 })`: 1 dəqiqə ərzində maksimum 3 dəfə qeydiyyat cəhdi edilə bilər.
   * `@Throttle({ limit: 5, ttl: 60000 })`: 1 dəqiqə ərzində maksimum 5 dəfə login cəhdi edilə bilər.
   * Artıq sorğu göndərən botlar birbaşa Gateway səviyyəsində bloklanır və daxili `auth-service`-i yormur!
3. **Mərkəzi Xəta İdarəçiliyi (`handleError`):** Daxili servis sönülü olarsa və ya xəta baş verərsə, Gateway təmiz `503 Service Unavailable` və ya daxili xəta mesajını istifadəçiyə çatdırır.

---

## 🔄 Məlumat Axını Sxemi (Data Flow)

```text
[Müştəri: Postman / Brauzer]
             │
             │  POST http://localhost:3000/auth/register
             ▼
┌──────────────────────────────────────────────┐
│            API GATEWAY (Port 3000)           │
│  1. Rate Limit yoxlanışı (@Throttle)         │
│  2. DTO Validasiyası (ValidationPipe)        │
│  3. HttpService (Axios) ilə yönləndirmə     │
└──────────────────────┬───────────────────────┘
                       │
                       │  HTTP POST http://localhost:3001/register
                       ▼
┌──────────────────────────────────────────────┐
│         AUTH MICROSERVICE (Port 3001)        │
│  1. PostgreSQL yoxlanışı və yazılışı        │
│  2. Şifrənin Bcrypt ilə hash-lənməsi         │
│  3. Kafka-ya 'user.registered' eventi atılır │
└──────────────────────┬───────────────────────┘
                       │
                       │  { message: "User registered successfully", userId }
                       ▼
            [Cavab Müştəriyə Qaytarılır]
```

---

## 🛠️ Modulun Fayl Strukturu və Rolları

| Fayl | Rolu | İzahı |
| :--- | :--- | :--- |
| **`auth.controller.ts`** | Qəbul Qapısı | `/auth/register`, `/auth/login`, `/auth/profile` endpoint-lərini təyin edir və `@Throttle` ilə sorğu sayını məhdudlaşdırır. |
| **`auth.service.ts`** | Yönləndirici (Proxy) | `@nestjs/axios` (`HttpService`) vasitəsilə sorğunu `http://localhost:3001` ünvanına ötürür və cavabı geri alır. |
| **`auth.module.ts`** | Təchizatçı | `HttpModule`-u import edərək Axios mühərrikini bu modula bağlayır. |

---

## 💡 Kod daxilindəki vacib terminlər:

* **`HttpService` (Axios):** NestJS-in digər serverlərə və mikroservislərə HTTP sorğusu atması üçün istifadə etdiyi müştəri.
* **`firstValueFrom(...)`:** Axios sorğuları RxJS `Observable` qaytarır. `firstValueFrom` onu müasir `async / await` (Promise) strukturuna çevirir ki, kodu rahatlıqla `await` edə bilək.
* **`SERVICES_PORTS.AUTH_SERVICE`:** Portları manual yazmamaq üçün `@app/common` kitabxanasından götürülən qlobal sabitdir (`3001`).

---

## 🧪 Gateway Üzərindən Test Nümunəsi:

Artıq birbaşa `3001`-ə deyil, **`3000` (Gateway)** portuna sorğu atırıq:

```bash
# 1. Gateway üzərindən Qeydiyyat:
POST http://localhost:3000/auth/register
Content-Type: application/json

{
  "email": "gateway_user@example.com",
  "password": "password123",
  "name": "Gateway User"
}
```
