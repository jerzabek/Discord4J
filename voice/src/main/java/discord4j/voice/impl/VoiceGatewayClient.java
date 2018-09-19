/*
 * This file is part of Discord4J.
 *
 * Discord4J is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Discord4J is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with Discord4J.  If not, see <http://www.gnu.org/licenses/>.
 */
package discord4j.voice.impl;

import discord4j.common.ResettableInterval;
import discord4j.voice.*;
import discord4j.voice.json.Identify;
import discord4j.voice.json.VoiceGatewayPayload;
import discord4j.voice.json.VoiceOpcode;
import reactor.core.Disposable;
import reactor.core.publisher.EmitterProcessor;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;
import reactor.ipc.netty.http.client.HttpClient;

class VoiceGatewayClient {

    static final String VERSION = "3";

    private final ResettableInterval heartbeat = new ResettableInterval();

    private final EmitterProcessor<VoiceGatewayPayload<?>> sender = EmitterProcessor.create(false);
    private final FluxSink<VoiceGatewayPayload<?>> senderSink = sender.sink(FluxSink.OverflowStrategy.LATEST);

    private final VoicePayloadReader payloadReader;
    private final VoicePayloadWriter payloadWriter;
    private final VoicePayloadHandler payloadHandler;

    VoiceGatewayClient(VoiceConnection voiceConnection, VoicePayloadReader payloadReader,
                       VoicePayloadWriter payloadWriter, String endpoint, Identify identify) {
        this.payloadReader = payloadReader;
        this.payloadWriter = payloadWriter;

        this.payloadHandler = new VoicePayloadHandler(voiceConnection, heartbeat, endpoint,
                                                      new VoiceGatewayPayload<>(VoiceOpcode.IDENTIFY, identify));
    }

    Mono<Void> execute(String gatewayUrl) {
        return Mono.defer(() -> {
            VoiceWebsocketHandler handler = new VoiceWebsocketHandler(payloadReader, payloadWriter);

            Disposable inboundSub = handler.inbound().subscribe(payloadHandler::handle);

            Disposable heartbeatSub = heartbeat.ticks()
                    .map(VoiceGatewayPayload::heartbeat)
                    .subscribe(handler.outbound()::onNext);

            Disposable senderSub = sender.subscribe(handler.outbound()::onNext, t -> handler.close(), handler::close);

            return HttpClient.create()
                    .ws(gatewayUrl)
                    .flatMap(response -> response.receiveWebsocket(handler::handle))
                    .doOnTerminate(() -> {
                        inboundSub.dispose();
                        senderSub.dispose();
                        heartbeatSub.dispose();
                        heartbeat.stop();
                    });
        });
    }

    FluxSink<VoiceGatewayPayload<?>> sender() {
        return senderSink;
    }
}
