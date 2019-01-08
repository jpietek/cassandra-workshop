package org.photoarchive.dao;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.mapping.MappingManager;
import org.photoarchive.model.MediaType;

import java.util.Date;
import java.util.List;

import static com.datastax.driver.core.querybuilder.QueryBuilder.*;

public class MediaFilteringQueries {

    private MappingManager manager;

    private Session session;
    private Cluster cluster;

    public MediaFilteringQueries() {
        session = Cassandra.INSTANCE.getSession();
        cluster = Cassandra.INSTANCE.getCluster();
        manager = new MappingManager(session);
    }

    public List<Row> listDirectorySortByModified(String path) {
        return Cassandra.INSTANCE.getSession().execute(
                select().column("name").column("path").column("modified")
                        .from("media_by_modified")
                        .where(eq("path", path)).and(gt("modified", new Date()))
                        .orderBy(desc("modified"))).all();
    }

    public List<Row> listDirectory(String path) {
        return Cassandra.INSTANCE.getSession().execute(
                select().column("name")
                        .from(Cassandra.MEDIA_BY_FOLDERS_TABLE_NAME)
                        .where(eq("path", path))).all();
    }

    public List<Row> sizeGreaterThan(String path, int size) {
        return Cassandra.INSTANCE.getSession().execute(select().column("path")
                .column("size")
                .from("media_by_size")
                .where(eq("path", path)).and(gt("size", size)).orderBy(desc("size"))
        ).all();
    }

    public List<Row> aggregateFolderSize() {
        return Cassandra.INSTANCE.getSession().execute(select().column("path")
                .sum(column("size"))
                .from("media_by_folders")
                .groupBy("path")
        ).all();
    }

    public List<Row> countExtensionsByType(MediaType type) {
        return Cassandra.INSTANCE.getSession().execute(select()
                .column("type")
                .column("extension")
                .count(column("extension"))
                .from("media_by_type")
                .where(eq("type", type))
                .groupBy("extension")
        ).all();
    }

}
