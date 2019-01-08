package org.photoarchive.factories;

import com.dropbox.core.v2.files.FileMetadata;
import com.dropbox.core.v2.files.MediaMetadata;
import com.dropbox.core.v2.files.VideoMetadata;
import org.apache.commons.io.FilenameUtils;
import org.photoarchive.model.Media;
import org.photoarchive.model.MediaType;

import java.util.Arrays;
import java.util.HashSet;

import static java.util.Objects.isNull;

public class MediaFactory {

    private MediaFactory() {

    }

    private static void parseFolderStructure(Media media) {
        String trimmedPath = FilenameUtils.getFullPath(media.getPath().trim());

        String[] folders = trimmedPath.split("/");
        int subFoldersCount = folders.length;

        if (subFoldersCount == 0) media.setCurrentDirectory("/");
        else media.setCurrentDirectory(folders[subFoldersCount - 1]);

        if (subFoldersCount > 1) media.setParentDirectory(folders[subFoldersCount - 2]);
        media.setDepth(subFoldersCount);

        media.setTags(new HashSet<>(Arrays.asList(folders)));
    }

    public static Media fromDropboxMetadata(FileMetadata metaData) {
        Media media = Media.builder().id(metaData.getId())
                .modified(metaData.getClientModified().toInstant())
                .name(metaData.getName())
                .extension(FilenameUtils.getExtension(metaData.getName()))
                .size(metaData.getSize())
                .path(FilenameUtils.getPath(metaData.getPathLower())).build();

        parseFolderStructure(media);

        if (!isNull(metaData.getMediaInfo())) {
            MediaMetadata mediaMetaData = metaData.getMediaInfo().getMetadataValue();
            media.setType(MediaType.IMAGE);

            if (!isNull(mediaMetaData.getDimensions())) {
                media.setDimensionsSet(true);
                media.setWidth(mediaMetaData.getDimensions().getWidth());
                media.setHeight(mediaMetaData.getDimensions().getHeight());
            }
            if (!isNull(mediaMetaData.getTimeTaken())) {
                media.setTimeTakenSet(true);
                media.setTimeTaken(mediaMetaData.getTimeTaken().toInstant());
            }

            if (!isNull(mediaMetaData.getLocation())) {
                media.setGpsSet(true);
                media.setGpsLatitude(mediaMetaData.getLocation().getLatitude());
                media.setGpsLongitude(mediaMetaData.getLocation().getLongitude());
            }

            if (mediaMetaData instanceof VideoMetadata) {
                media.setType(MediaType.VIDEO);

                VideoMetadata videoMetaData = (VideoMetadata) mediaMetaData;
                if (!isNull(videoMetaData.getDuration())) {
                    media.setVideoDuration(videoMetaData.getDuration());
                }
            }
        }

        return media;
    }
}
