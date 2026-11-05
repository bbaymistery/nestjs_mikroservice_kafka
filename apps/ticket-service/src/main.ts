import { NestFactory } from '@nestjs/core';
import { TicketsServiceModule } from './tickets-service.module';
import { SERVICES_PORTS } from '@app/common';

async function bootstrap() {
  const app = await NestFactory.create(TicketsServiceModule);
  await app.listen(process.env.PORT ?? SERVICES_PORTS.TICKETS_SERVICE);
  console.log(`Tickets service is running on port ${process.env.PORT ?? SERVICES_PORTS.TICKETS_SERVICE}`);
}
bootstrap();
