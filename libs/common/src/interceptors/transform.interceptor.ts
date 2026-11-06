import {
  Injectable,
  NestInterceptor,
  ExecutionContext,
  CallHandler,
} from '@nestjs/common';
import { Observable } from 'rxjs';
import { map } from 'rxjs/operators';
import { Request } from 'express';

export interface ApiResponse<T> {
  success: boolean;
  data: T;
  meta: {
    timestamp: string;
    path: string;
    method: string;
  };
}
/*
Bütün API Gateway-ə qanun qoydun:

"Hansı servisdən nə cavab gəlir-gəlsin, Gateway-dən çölə (müştəriyə) çıxanda
 mütləq eyni gözəl qutuya bükülüb çıxmalıdır!"

İndi sən hansı endpoint-i çağırırsan-çağır, istifadəçiyə cavab həmişə eyni 
standartda gələcək:

Nümunə 1: Konsertləri çağıranda (GET /events):
json
{
  "success": true,
  "data": [
    { "title": "Tarkan Konserti", "price": 50 }
  ],
  "meta": {
    "timestamp": "2026-11-06T10:15:30.000Z",
    "path": "/events",
    "method": "GET"
  }
}

*/
@Injectable()
export class TransformInterceptor<T> implements NestInterceptor<T, ApiResponse<T>> {
  intercept(context: ExecutionContext, next: CallHandler): Observable<ApiResponse<T>> {
    const request: Request = context.switchToHttp().getRequest();
    const { method, url } = request;

    return next.handle().pipe(
      map((data: T) => ({
        success: true,
        data,
        meta: {
          timestamp: new Date().toISOString(),
          path: url,
          method,
        },
      })),
    );
  }
}
