package org.photoarchive.dao;

import com.datastax.driver.core.Session;
import com.datastax.driver.mapping.Mapper;
import com.datastax.driver.mapping.MappingManager;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static com.datastax.driver.mapping.Mapper.Option.saveNullFields;

public class CrudDaoImpl<T> implements CrudDao<T> {

    private Mapper<T> mapper;

    public CrudDaoImpl(Class<T> klass) {
        Session session = Cassandra.INSTANCE.getSession();
        MappingManager manager = new MappingManager(session);
        mapper = manager.mapper(klass);
    }

    public void save(T entity) {
        mapper.save(entity, saveNullFields(false));
    }

    public void batchSave(Set<T> entities) {
        List<CompletableFuture<Void>> futures =
                entities.stream().map(e -> CompletableFuture.runAsync(
                        () -> mapper.save(e)
                )).collect(Collectors.toList());

        futures.forEach(CompletableFuture::join);
    }

    public Optional<T> get(String... keys) {
        return Optional.ofNullable(mapper.get(keys));
    }
}