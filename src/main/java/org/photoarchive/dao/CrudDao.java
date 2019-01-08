package org.photoarchive.dao;

import java.util.List;
import java.util.Optional;
import java.util.Set;

public interface CrudDao<T> {

    void save(T object);
    Optional<T> get(String... keys);
    void batchSave(Set<T> entities);
}
