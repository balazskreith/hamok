package io.github.balazskreith.hamok.raccoons;

import java.util.UUID;

record RemotePeer(UUID id, boolean active, Long touched) {

}
