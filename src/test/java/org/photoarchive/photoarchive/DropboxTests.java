package org.photoarchive.photoarchive;

import com.dropbox.core.DbxException;
import lombok.extern.java.Log;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.photoarchive.cloud.dropbox.DropboxService;
import org.photoarchive.cloud.dropbox.ListRequest;
import org.photoarchive.dao.Cassandra;
import org.photoarchive.dao.CrudDao;
import org.photoarchive.dao.CrudDaoImpl;
import org.photoarchive.model.Collection;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@Log
public class DropboxTests {

    private static final int MAX_FILES_PER_LIST_REQUEST = 2000;
    private DropboxService dropboxService;
    private CrudDao<Collection> collectionCrudDao = new CrudDaoImpl<>(Collection.class);

    @BeforeClass
    public static void cassandraSetup() {
        Cassandra.INSTANCE.setup(true);
    }

    @Before
    public void dropboxSetup() {
        String token = System.getenv("DROPBOX_TOKEN");
        dropboxService = new DropboxService(token);
    }

    @Test
    public void listFolder() {
        String path = "/Alicja";
        ListRequest req = ListRequest.builder().folderPath(path)
                .recursive(true).limit(MAX_FILES_PER_LIST_REQUEST).build();
        try {
            dropboxService.persistFolderToDb(req);
            assertTrue(collectionCrudDao.get(path).isPresent());
        } catch (DbxException e) {
            fail("got dbx exception: " + e.getMessage());
        }
    }

}
