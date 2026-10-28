import { NestFactory } from '@nestjs/core';
import { AppModule } from './app.module';
import { SERVICES_PORTS } from '@app/common';

async function bootstrap() {
  const app = await NestFactory.create(AppModule);
  await app.listen(process.env.PORT || SERVICES_PORTS.API_GATEWAY);
  console.log(
    `API Gateway is running on port ${process.env.PORT || SERVICES_PORTS.API_GATEWAY}`,
  );
}
bootstrap();
