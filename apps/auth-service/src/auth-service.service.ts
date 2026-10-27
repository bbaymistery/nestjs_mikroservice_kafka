import { Inject, Injectable } from '@nestjs/common';
import { KAFKA_SERVICE, KAFKA_TOPICS } from '@app/kafka';
import { ClientKafka } from '@nestjs/microservices';

@Injectable()
export class AuthServiceService {
  constructor(
    @Inject(KAFKA_SERVICE) private readonly kafkaClient: ClientKafka
  ) { }

  async onModuleInit() {
    //connect to kafka when module initializies
    await this.kafkaClient.connect();

  }
  getHello(): string {
    return 'Hello World!';
  }

  async simulateUserRegistration(email: string) {
    //publish event to kafka 
    this.kafkaClient.emit(KAFKA_TOPICS.USER_REGISTERED, {
      email,
      timestamp: new Date().toISOString()
    });
    return `Event for user ${email} has been published to Kafka`;
  }
}
