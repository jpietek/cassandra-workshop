package org.photoarchive.photoarchive;

import com.datastax.driver.core.Row;
import lombok.extern.java.Log;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.photoarchive.dao.Cassandra;
import org.photoarchive.dao.CrudDao;
import org.photoarchive.dao.CrudDaoImpl;
import org.photoarchive.dao.MediaFilteringQueries;
import org.photoarchive.model.Media;
import org.photoarchive.model.MediaType;

import java.util.*;

import static org.junit.Assert.*;

@Log
public class CassandraIntegrationTests {

    private MediaFilteringQueries mediaFilteringQueries = new MediaFilteringQueries();
    private CrudDao<Media> mediaCrudDao = new CrudDaoImpl<>(Media.class);

    private Media testMedia = Media.builder().name("testmedia").path("/root/").extension(".jpg").id("axcasd")
            .type(MediaType.IMAGE).build();

    //@ClassRule
    //public static CassandraContainer cassandraContainer = new CassandraContainer();

    @BeforeClass
    public static void init() {
        //Cluster dockerizedCluster = cassandraContainer.getCluster();
        //Cassandra.INSTANCE.setCluster(dockerizedCluster);
        Cassandra.INSTANCE.setup(true);
    }

    @Before
    public void setup() {

    }

    @Test
    public void saveAndLoadMedia() {
        mediaCrudDao.save(testMedia);
        String name = testMedia.getName();
        String path = testMedia.getPath();
        String id = testMedia.getId();
        String extension = testMedia.getExtension();

        Optional<Media> media = mediaCrudDao.get(path, name, id, extension);
        assertTrue(media.isPresent());
    }

    @Test
    public void batchMediaSave() {
        testMedia.setName("batch1");
        testMedia.setName("batch2");
        mediaCrudDao.batchSave(new HashSet<>(Arrays.asList(testMedia, testMedia)));
    }

    @Test
    public void modifiedSort() {
        Calendar cal = Calendar.getInstance();
        testMedia.setModified(cal.getTime().toInstant());
        testMedia.setPath("/root/modified");
        testMedia.setName("mod1");
        mediaCrudDao.save(testMedia);

        cal.add(Calendar.HOUR, -1);
        testMedia.setModified(cal.getTime().toInstant());
        testMedia.setPath("/root/modified");
        testMedia.setName("mod2");
        mediaCrudDao.save(testMedia);

        cal.add(Calendar.HOUR, 5);
        testMedia.setModified(cal.getTime().toInstant());
        testMedia.setPath("/root/modified");
        testMedia.setName("mod3");
        mediaCrudDao.save(testMedia);

        List<Row> out = mediaFilteringQueries.listDirectorySortByModified("/root/modified");
        out.forEach(r -> log.info(r.toString()));
        assertEquals(1, out.size());
    }

    @Test
    public void listFolder() {
        testMedia.setName("ble1");
        testMedia.setPath("/root/test");
        testMedia.setSize(1);
        mediaCrudDao.save(testMedia);

        testMedia.setName("ble2");
        testMedia.setPath("/root/test");
        testMedia.setSize(2);
        mediaCrudDao.save(testMedia);

        testMedia.setName("ble3");
        testMedia.setPath("/root/test");
        testMedia.setSize(3);
        mediaCrudDao.save(testMedia);

        List<Row> out = mediaFilteringQueries.listDirectory("/root/test");
        out.forEach(r -> log.info(r.toString()));
        assertEquals(3, out.size());
    }

    @Test
    public void aggregateFolderSize() {
        List<Row> out = mediaFilteringQueries.aggregateFolderSize();
        out.forEach(r -> log.info(r.toString()));
        assertFalse(out.isEmpty());
    }

    @Test
    public void countExtensionsByType() {
        List<Row> out = mediaFilteringQueries.countExtensionsByType(MediaType.VIDEO);
        out.forEach(r -> log.info(r.toString()));
        assertFalse(out.isEmpty());
    }
}
