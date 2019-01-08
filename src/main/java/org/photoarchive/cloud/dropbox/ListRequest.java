package org.photoarchive.cloud.dropbox;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

import java.util.Optional;

@Data
@Builder
@AllArgsConstructor
public class ListRequest {

    private String folderPath;
    private long limit;
    private boolean recursive;

    @Builder.Default
    private Optional<String> latestCursor = Optional.empty();
}
