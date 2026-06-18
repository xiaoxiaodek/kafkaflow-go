
import { Injectable } from '@angular/core';
import { HttpClient } from '@angular/common/http';
import { ApiConfiguration } from './api-configuration';
import { TelemetryResponse } from './models/telemetry-response';
import { firstValueFrom } from 'rxjs';

@Injectable()
export class Gateway {
    constructor(
        private config: ApiConfiguration,
        private http: HttpClient
    ) {
    }

    async pauseConsumerTopic(groupId: string, consumerName: string, topicName: string): Promise<void> {
        await firstValueFrom(this.http.post(
            this.config.rootUrl + `/groups/${groupId}/consumers/${consumerName}/pause?topics=${topicName}`, ''));
    }

    async resumeConsumerTopic(groupId: string, consumerName: string, topicName: string): Promise<void> {
        await firstValueFrom(this.http.post(
            this.config.rootUrl + `/groups/${groupId}/consumers/${consumerName}/resume?topics=${topicName}`, ''));
    }

    async resetConsumerTopic(groupId: string, consumerName: string, topicName: string): Promise<void> {
        await firstValueFrom(this.http.post(
            this.config.rootUrl + `/groups/${groupId}/consumers/${consumerName}/reset-offsets`,
            { confirm: true, topics: [topicName] }));
    }

    async rewindConsumerTopic(groupId: string, consumerName: string, topicName: string, timestamp: number): Promise<void> {
        await firstValueFrom(this.http.post(
            this.config.rootUrl + `/groups/${groupId}/consumers/${consumerName}/rewind-offsets-to-date`,
            { timestamp: timestamp, topics: [topicName] }));
    }

    async stopConsumer(groupId: string, consumerName: string): Promise<void> {
        await firstValueFrom(this.http.post(
            this.config.rootUrl + `/groups/${groupId}/consumers/${consumerName}/stop`, ''));
    }

    async startConsumer(groupId: string, consumerName: string): Promise<void> {
        await firstValueFrom(this.http.post(
            this.config.rootUrl + `/groups/${groupId}/consumers/${consumerName}/start`, ''));
    }

    async restartConsumer(groupId: string, consumerName: string): Promise<void> {
        await firstValueFrom(this.http.post(
            this.config.rootUrl + `/groups/${groupId}/consumers/${consumerName}/restart`, ''));
    }

    async changeWorkers(groupId: string, consumerName: string, workersCount: number): Promise<void> {
        await firstValueFrom(this.http.post(
            this.config.rootUrl + `/groups/${groupId}/consumers/${consumerName}/change-worker-count`,
            { workers_count: workersCount }));
    }

    async getTelemetry(): Promise<TelemetryResponse> {
        return await firstValueFrom(this.http.get<TelemetryResponse>(
            this.config.rootUrl + `/consumers/telemetry`));
    }
}
