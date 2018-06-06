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
import discord4j.voice.VoiceConnection;
import discord4j.voice.json.*;
import reactor.util.Logger;
import reactor.util.Loggers;

import java.time.Duration;

class VoicePayloadHandler {

    private static final Logger log = Loggers.getLogger(VoicePayloadHandler.class);

    private final VoiceConnection voiceConnection;
    private final ResettableInterval heartbeat;
    private final String endpoint;
    private final VoiceGatewayPayload<Identify> identifyPayload;

    private int ssrc = 0;

    VoicePayloadHandler(VoiceConnection voiceConnection, ResettableInterval heartbeat,
                               String endpoint, VoiceGatewayPayload<Identify> identifyPayload) {
        this.voiceConnection = voiceConnection;
        this.heartbeat = heartbeat;
        this.endpoint = endpoint;
        this.identifyPayload = identifyPayload;
    }

    @SuppressWarnings("ConstantConditions")
    <T extends VoicePayloadData> void handle(VoiceGatewayPayload<T> payload) {
        switch (payload.getOp().getRawOp()) {
            case 2:  handleReady((Ready) payload.getData()); break;
            case 4:  handleSessionDescription((SessionDescription) payload.getData()); break;
            case 5:  handleSpeaking((Speaking) payload.getData()); break;
            case 6:  handleHeartbeatAck(); break;
            case 8:  handleHello((Hello) payload.getData()); break;
            case 9:  handleResumed(); break;
            case 13: handleClientDisconnect((ClientDisconnect) payload.getData()); break;
        }
    }

    private void handleReady(Ready payload) {
        log.info("READY");

        this.ssrc = payload.getSsrc();

        voiceConnection.setupUdp(endpoint, payload.getPort())
                .then(voiceConnection.discoverIp(payload.getSsrc()))
                .map(t -> VoiceGatewayPayload.selectProtocol("udp", t.getT1(), t.getT2(), VoiceSocket.ENCRYPTION_MODE))
                .subscribe(voiceConnection::sendGatewayMessage);
    }

    private void handleSessionDescription(SessionDescription payload) {
        log.info("SESSION DESCRIPTION");

        voiceConnection.startHandlingAudio(payload.getSecretKey(), ssrc);
    }

    private void handleSpeaking(Speaking payload) {
        log.info("SPEAKING");
    }

    private void handleHeartbeatAck() {
        log.info("HEARTBEAT ACK");
    }

    private void handleHello(Hello payload) {
        log.info("HELLO");

        Duration interval = Duration.ofMillis(payload.getHeartbeatInterval());
        heartbeat.start(interval);

        voiceConnection.sendGatewayMessage(identifyPayload);
    }

    private void handleResumed() {
        log.info("RESUMED");
    }

    private void handleClientDisconnect(ClientDisconnect payload) {
        log.info("CLIENT DISCONNECT");
    }
}
