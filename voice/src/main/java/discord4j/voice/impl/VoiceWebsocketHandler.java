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

import discord4j.voice.VoicePayloadReader;
import discord4j.voice.VoicePayloadWriter;
import discord4j.voice.json.VoiceGatewayPayload;
import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketFrame;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.UnicastProcessor;
import reactor.ipc.netty.NettyPipeline;
import reactor.ipc.netty.http.websocket.WebsocketInbound;
import reactor.ipc.netty.http.websocket.WebsocketOutbound;
import reactor.util.Logger;
import reactor.util.Loggers;

import java.util.logging.Level;

class VoiceWebsocketHandler {

    private static final Logger inboundLogger = Loggers.getLogger("discord4j.voice.inbound");
    private static final Logger outboundLogger = Loggers.getLogger("discord4j.voice.outbound");

    private final UnicastProcessor<VoiceGatewayPayload<?>> inboundExchange = UnicastProcessor.create();
    private final UnicastProcessor<VoiceGatewayPayload<?>> outboundExchange = UnicastProcessor.create();

    private final VoicePayloadReader reader;
    private final VoicePayloadWriter writer;

    VoiceWebsocketHandler(VoicePayloadReader reader, VoicePayloadWriter writer) {
        this.reader = reader;
        this.writer = writer;
    }

    public Mono<Void> handle(WebsocketInbound in, WebsocketOutbound out) {
        in.aggregateFrames()
                .receiveFrames()
                .map(WebSocketFrame::content)
                .map(reader::read)
                .log(inboundLogger, Level.FINE, false)
                .subscribe(inboundExchange::onNext, this::error);

        return out.options(NettyPipeline.SendOptions::flushOnEach)
                .sendObject(outboundExchange.map(this::mapOutbound)
                        .log(outboundLogger, Level.FINE, false)
                        .map(TextWebSocketFrame::new))
                .then();
    }

    Flux<VoiceGatewayPayload<?>> inbound() {
        return inboundExchange;
    }

    UnicastProcessor<VoiceGatewayPayload<?>> outbound() {
        return outboundExchange;
    }

    void close() {
        outboundExchange.onComplete();
        inboundExchange.onComplete();
    }

    private void error(Throwable error) {
        outboundExchange.onComplete();
        inboundExchange.onComplete();
    }

    private ByteBuf mapOutbound(VoiceGatewayPayload<?> payload) {
        return writer.write(payload);
    }
}
