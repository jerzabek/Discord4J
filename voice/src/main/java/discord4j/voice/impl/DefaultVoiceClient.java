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

import discord4j.voice.VoiceClient;
import discord4j.voice.VoiceConnection;
import discord4j.voice.VoicePayloadReader;
import discord4j.voice.VoicePayloadWriter;
import discord4j.voice.json.Identify;

import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

class DefaultVoiceClient implements VoiceClient {

    private final ConcurrentHashMap<Long, VoiceConnection> connections = new ConcurrentHashMap<>();
    private final VoicePayloadReader payloadReader;
    private final VoicePayloadWriter payloadWriter;

    DefaultVoiceClient(VoicePayloadReader payloadReader, VoicePayloadWriter payloadWriter) {
        this.payloadReader = payloadReader;
        this.payloadWriter = payloadWriter;
    }

    @Override
    public VoiceConnection newConnection(String endpoint, long guildId, long userId, String sessionId, String token) {

        if (connections.containsKey(guildId)) {
            throw new IllegalArgumentException("Attempt to create voice connection for guild with existing connection");
        }

        Identify identify = new Identify(Long.toUnsignedString(guildId), Long.toUnsignedString(userId), sessionId,
                                         token);

        DefaultVoiceConnection connection = new DefaultVoiceConnection(payloadReader, payloadWriter, endpoint,
                                                                       identify);

        connections.put(guildId, connection);

        return connection;
    }

    @Override
    public Optional<VoiceConnection> getConnection(long guildId) {
        return Optional.ofNullable(connections.get(guildId));
    }
}
