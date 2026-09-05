package com.csg.airtel.aaa4j.application.consumer;

import com.csg.airtel.aaa4j.domain.model.connectionhistory.AccountingEvent;
import io.quarkus.kafka.client.serialization.ObjectMapperDeserializer;

/**
 * Deserializes the incoming CDR records into {@link AccountingEvent}.
 *
 * <p>Kafka instantiates deserializers reflectively through their public no-argument constructor,
 * which {@code ObjectMapperDeserializer} does not have — it only takes the target type. So the
 * type is bound here, in a subclass the Kafka client can construct, and the incoming channels
 * name this class instead of the base one. The parsing itself is unchanged: the base class still
 * uses the application {@code ObjectMapper}, the same one the outgoing channel's
 * {@code ObjectMapperSerializer} republishes the event with.
 */
public class AccountingEventDeserializer extends ObjectMapperDeserializer<AccountingEvent> {

    public AccountingEventDeserializer() {
        super(AccountingEvent.class);
    }
}
