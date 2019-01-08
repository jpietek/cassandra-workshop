package org.photoarchive.model;

import com.datastax.driver.mapping.annotations.PartitionKey;
import com.datastax.driver.mapping.annotations.Table;
import lombok.*;
import org.photoarchive.dao.Cassandra;

import java.io.Serializable;
import java.util.Optional;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder(toBuilder=true)
@EqualsAndHashCode
@ToString

@Table(name = Cassandra.COLLECTIONS_TABLE_NAME)
public class Collection implements Serializable {

    @PartitionKey
    String path;

    @Builder.Default
    Optional<String> lastDropboxCursor = Optional.empty();

    long itemCount;
}
