package org.photoarchive.model;

import com.datastax.driver.mapping.annotations.ClusteringColumn;
import com.datastax.driver.mapping.annotations.PartitionKey;
import com.datastax.driver.mapping.annotations.Table;
import lombok.*;
import org.photoarchive.dao.Cassandra;

import java.io.Serializable;
import java.time.Instant;
import java.util.Set;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder(toBuilder=true)
@EqualsAndHashCode
@ToString

@Table(name = Cassandra.MEDIA_BY_FOLDERS_TABLE_NAME)
public class Media implements Serializable {

    @PartitionKey
    String path;

    @ClusteringColumn
    int depth;

    String currentDirectory;
    String parentDirectory;

    @ClusteringColumn(1)
    String name;

    @ClusteringColumn(2)
    String id;

    @ClusteringColumn(3)
    String extension;

    Instant modified;
    Instant timeTaken;

    Set<String> tags;

    long size;

    long width;
    long height;

    double gpsLatitude;
    double gpsLongitude;

    boolean gpsSet;
    boolean dimensionsSet;
    boolean timeTakenSet;

    MediaType type;

    long videoDuration;
    String videoThumbnailLink;
}
