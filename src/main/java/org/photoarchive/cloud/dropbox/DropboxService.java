package org.photoarchive.cloud.dropbox;

import com.dropbox.core.DbxException;
import com.dropbox.core.DbxRequestConfig;
import com.dropbox.core.v2.DbxClientV2;
import com.dropbox.core.v2.files.FileMetadata;
import com.dropbox.core.v2.files.ListFolderResult;
import lombok.extern.java.Log;
import org.photoarchive.dao.CrudDao;
import org.photoarchive.dao.CrudDaoImpl;
import org.photoarchive.factories.MediaFactory;
import org.photoarchive.model.Collection;
import org.photoarchive.model.Media;

import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

@Log
public class DropboxService {

    private DbxClientV2 client;

    private CrudDao<Collection> collectionCrudDao = new CrudDaoImpl<>(Collection.class);
    private CrudDao<Media> mediaCrudDao = new CrudDaoImpl<>(Media.class);

    public DropboxService(String token) {
        DbxRequestConfig config = DbxRequestConfig.newBuilder("photo-archive/0.1").build();
        client = new DbxClientV2(config, token);
    }

    public void persistFolderToDb(ListRequest req) throws DbxException {

        ListFolderResult result;
        do {
            Collection collection = collectionCrudDao.get(req.getFolderPath())
                    .orElse(Collection.builder().path(req.getFolderPath()).build());

            if (collection.getLastDropboxCursor().isPresent()) {
                log.info("reusing dropbox token");
                result = client.files().listFolderContinue(collection.getLastDropboxCursor().get());
            } else {
                result = client.files().listFolderBuilder(req.getFolderPath())
                        .withIncludeDeleted(false).withIncludeMediaInfo(true)
                        .withRecursive(req.isRecursive()).withLimit(req.getLimit())
                        .start();
            }

            Set<Media> mediaToPersist = result.getEntries().stream()
                    .filter(metaData -> metaData instanceof FileMetadata)
                    .map(metaData -> (FileMetadata) metaData)
                    .map(MediaFactory::fromDropboxMetadata)
                    .collect(Collectors.toSet());

            mediaCrudDao.batchSave(mediaToPersist);

            collection.setLastDropboxCursor(Optional.of(result.getCursor()));
            collection.setItemCount(collection.getItemCount() + mediaToPersist.size());
            collectionCrudDao.save(collection);

        } while (result.getHasMore());

    }
}
