import { Injectable } from '@nestjs/common';
import { SERVICES_PORTS } from '@app/common';

@Injectable()
export class AppService {
  getHello(): string {
    return `Api gateway is running on port ${SERVICES_PORTS.API_GATEWAY}`;
  }
}
