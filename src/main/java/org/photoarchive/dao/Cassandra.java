package org.photoarchive.dao;

import com.datastax.driver.core.*;
import com.datastax.driver.extras.codecs.enums.EnumNameCodec;
import com.datastax.driver.extras.codecs.jdk8.InstantCodec;
import com.datastax.driver.extras.codecs.jdk8.OptionalCodec;
import lombok.Setter;
import lombok.extern.java.Log;
import org.photoarchive.model.MediaType;

import static com.datastax.driver.core.schemabuilder.SchemaBuilder.createTable;
import static java.util.Objects.isNull;

@Log
public enum Cassandra {

    INSTANCE;

    public static final String KEYSPACE = "test_media_archive";
    public static final String HOST = "localhost";

    private Session session;
    @Setter
    private Cluster cluster;

    public static final String MEDIA_BY_FOLDERS_TABLE_NAME = "media_by_folders";
    public static final String COLLECTIONS_TABLE_NAME = "collections";

    private void setupCodecs(Cluster cluster) {
        CodecRegistry registry = cluster.getConfiguration().getCodecRegistry();
        registry.register(new EnumNameCodec<>(MediaType.class));
        registry.register(new OptionalCodec<>(TypeCodec.varchar()));
        registry.register(InstantCodec.instance);
    }

    private boolean tableNotCreated(String table) {
        KeyspaceMetadata ks = cluster.getMetadata().getKeyspace(Cassandra.KEYSPACE);
        return ks.getTable(table) == null;
    }

    private void dropExistingTables() {
        session.execute("DROP MATERIALIZED VIEW IF EXISTS media_by_modified;");
        session.execute("DROP MATERIALIZED VIEW IF EXISTS media_by_size;");
        session.execute("DROP MATERIALIZED VIEW IF EXISTS media_by_type;");
        session.execute("DROP TABLE IF EXISTS media_by_folders;");
        session.execute("DROP TABLE IF EXISTS collections;");
    }

    private void createMaterializedViews() {
        String mediaByModified = "CREATE MATERIALIZED VIEW IF NOT EXISTS media_by_modified AS\n" +
                " SELECT path, name, modified, extension" +
                " FROM media_by_folders" +
                " WHERE name IS NOT NULL" +
                " AND depth IS NOT NULL" +
                " AND extension IS NOT NULL" +
                " AND path IS NOT NULL" +
                " AND id IS NOT NULL" +
                " AND modified IS NOT NULL" +
                " PRIMARY KEY(path, depth, modified, name, id, extension);";

        String mediaBySize = "CREATE MATERIALIZED VIEW IF NOT EXISTS media_by_extension AS\n" +
                " SELECT extension, size" +
                " FROM media_by_folders" +
                " WHERE name IS NOT NULL" +
                " AND depth IS NOT NULL" +
                " AND extension IS NOT NULL" +
                " AND path IS NOT NULL" +
                " AND id IS NOT NULL" +
                " AND size IS NOT NULL" +
                " PRIMARY KEY(extension, size, depth, path, name, id);";

        String mediaByType = "CREATE MATERIALIZED VIEW IF NOT EXISTS media_by_type AS\n" +
                " SELECT path, name, size, type, extension" +
                " FROM media_by_folders" +
                " WHERE type IS NOT NULL" +
                " AND depth IS NOT NULL" +
                " AND extension IS NOT NULL" +
                " AND name IS NOT NULL" +
                " AND path IS NOT NULL" +
                " AND id IS NOT NULL" +
                " AND size IS NOT NULL" +
                " PRIMARY KEY(type, depth, extension, path, name, id);";

        Cassandra.INSTANCE.getSession().execute(mediaByModified);
        Cassandra.INSTANCE.getSession().execute(mediaBySize);
        Cassandra.INSTANCE.getSession().execute(mediaByType);
    }

    public void setup(boolean dropTables) {
        if (isNull(cluster)) {
            cluster = Cluster.builder().withoutJMXReporting().addContactPoint(HOST).build();
        }

        setupCodecs(cluster);

        if (isNull(session)) {
            Session tmpSession = cluster.connect();
            tmpSession.execute("CREATE KEYSPACE IF NOT EXISTS " + KEYSPACE + " WITH replication "
                    + "= {'class':'SimpleStrategy', 'replication_factor':1};");
            tmpSession.close();

            session = getCluster().connect(KEYSPACE);
        }

        if(dropTables) dropExistingTables();

        if (tableNotCreated(Cassandra.MEDIA_BY_FOLDERS_TABLE_NAME)) {
            session.execute(createTable(Cassandra.KEYSPACE, Cassandra.MEDIA_BY_FOLDERS_TABLE_NAME)
                    .addPartitionKey("path", DataType.text())
                    .addClusteringColumn("depth", DataType.cint())
                    .addClusteringColumn("name", DataType.text())
                    .addClusteringColumn("id", DataType.text())
                    .addClusteringColumn("extension", DataType.text())
                    .addColumn("currentDirectory", DataType.text())
                    .addColumn("parentDirectory", DataType.text())
                    .addColumn("modified", DataType.timestamp())
                    .addColumn("timetakenset", DataType.cboolean())
                    .addColumn("timeTaken", DataType.timestamp())
                    .addColumn("size", DataType.bigint())
                    .addColumn("dimensionsset", DataType.cboolean())
                    .addColumn("width", DataType.bigint())
                    .addColumn("height", DataType.bigint())
                    .addColumn("gpsLongitude", DataType.cdouble())
                    .addColumn("gpsLatitude", DataType.cdouble())
                    .addColumn("gpsset", DataType.cboolean())
                    .addColumn("type", DataType.text())
                    .addColumn("videoduration", DataType.bigint())
                    .addColumn("videothumbnaillink", DataType.text())
                    .addColumn("tags", DataType.set(DataType.text())));
        }

        if (tableNotCreated(Cassandra.COLLECTIONS_TABLE_NAME)) {
            session.execute(createTable(Cassandra.KEYSPACE, Cassandra.COLLECTIONS_TABLE_NAME)
                    .addPartitionKey("path", DataType.text())
                    .addColumn("lastDropboxCursor", DataType.varchar())
                    .addColumn("itemCount", DataType.bigint()));
        }

        createMaterializedViews();
    }

    public void disconnect() {
        if (!isNull(session)) {
            session.close();
        }
        if (!isNull(cluster)) {
            cluster.close();
        }
    }

    public Session getSession() {
        if (isNull(session)) {
            throw new IllegalStateException("No connection initialized");
        }
        return session;
    }

    public Cluster getCluster() {
        if (isNull(cluster)) {
            throw new IllegalStateException("No connection initialized");
        }
        return cluster;
    }

}
