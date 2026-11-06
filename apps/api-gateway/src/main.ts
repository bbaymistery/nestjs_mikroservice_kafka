import { NestFactory } from '@nestjs/core';
import { AppModule } from './app.module';
import { SERVICES_PORTS } from '@app/common';
import { ValidationPipe } from '@nestjs/common';
import { TransformInterceptor } from '@app/common/interceptors';
import { AllExceptionsFilter, HttpExceptionFilter } from '@app/common/filters';

async function bootstrap() {
  const app = await NestFactory.create(AppModule);
  //Enable validation
  app.useGlobalPipes(
    new ValidationPipe({
      whitelist: true,
      transform: true,
      forbidNonWhitelisted: true,
    }),
  );

  // Global interceptors, filters, and other middlewares can be set up here
  app.useGlobalInterceptors(new TransformInterceptor());


  //Note: Filters are applied in a reversed order
  app.useGlobalFilters(new AllExceptionsFilter(), new HttpExceptionFilter());


  await app.listen(process.env.PORT || SERVICES_PORTS.API_GATEWAY);
  console.log(
    `API Gateway is running on port ${process.env.PORT || SERVICES_PORTS.API_GATEWAY}`,
  );
}
bootstrap();
