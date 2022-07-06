package io.github.balazskreith.hamok.racoon;

import java.util.UUID;

record RemotePeer(UUID id, boolean active, Long touched) {

}
