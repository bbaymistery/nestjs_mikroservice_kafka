import { Body, Controller, Get, Post } from '@nestjs/common';
import { AuthServiceService } from './auth-service.service';

@Controller()
export class AuthServiceController {
  constructor(private readonly authServiceService: AuthServiceService) { }

  @Get()
  getHello(): string {
    return this.authServiceService.getHello();
  }

  @Post('register')
  async register(@Body('email') email: string) {
    return this.authServiceService.simulateUserRegistration(email);
  }
}
