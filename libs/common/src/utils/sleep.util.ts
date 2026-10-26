/**
 * Pauses execution asynchronously for a specified duration in milliseconds.
 * Commonly used for retry logic, backoff strategies, and throttling.
 */
/*
 * Kullanım Örnekleri (Mikroservisler & Kafka):
 * - Bağlantı koptuğunda yeniden denemeden önce beklemek (Retry / Exponential Backoff)
 *   örn: await sleep(3000); // 3 saniye bekle ve tekrar bağlanmayı dene
 * - İstek sınırlaması (Rate Limiting / Throttling)
 * - Testlerde ve simülasyonlarda gecikme yaratmak
*/
export const sleep = (ms: number): Promise<void> => {
  return new Promise((resolve) => setTimeout(resolve, ms));
};
