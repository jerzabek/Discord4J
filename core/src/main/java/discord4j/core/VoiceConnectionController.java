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
package discord4j.core;

import discord4j.gateway.json.GatewayPayload;
import discord4j.gateway.json.VoiceStateUpdate;
import discord4j.voice.AudioProvider;
import discord4j.voice.AudioReceiver;
import discord4j.voice.VoiceConnection;
import reactor.core.publisher.Mono;

public class VoiceConnectionController {

    private final VoiceConnection connection;
    private final ServiceMediator serviceMediator;
    private final long guildId;
    private final long channelId;

    private boolean selfMute;
    private boolean selfDeaf;

    public VoiceConnectionController(VoiceConnection connection, ServiceMediator serviceMediator, long guildId,
                                     long channelId, boolean selfMute, boolean selfDeaf) {
        this.connection = connection;
        this.serviceMediator = serviceMediator;
        this.guildId = guildId;
        this.channelId = channelId;
        this.selfMute = selfMute;
        this.selfDeaf = selfDeaf;
    }

    public Mono<Void> connect(AudioProvider audioProvider, AudioReceiver audioReceiver) {
        connection.setHandlers(audioProvider, audioReceiver);
        return connection.execute();
    }

    public void disconnect() {
        connection.shutdown();

        GatewayPayload<VoiceStateUpdate> update = GatewayPayload.voiceStateUpdate(new VoiceStateUpdate(guildId, null, selfMute, selfDeaf));
        serviceMediator.getGatewayClient().sender().next(update);
    }

    public void setSelfMute(boolean selfMute) {
        GatewayPayload<VoiceStateUpdate> update = GatewayPayload.voiceStateUpdate(new VoiceStateUpdate(guildId, channelId, selfMute, selfDeaf));
        serviceMediator.getGatewayClient().sender().next(update);
        this.selfMute = selfMute;
    }

    public void setSelfDeaf(boolean selfDeaf) {
        GatewayPayload<VoiceStateUpdate> update = GatewayPayload.voiceStateUpdate(new VoiceStateUpdate(guildId, channelId, selfMute, selfDeaf));
        serviceMediator.getGatewayClient().sender().next(update);
        this.selfDeaf = selfDeaf;
    }
}
