from typing import List

from google.cloud import datastore
from google.cloud.datastore import Key

from dittostore.objects import BaseEntity, AbstractDSEntity


class DittoStore:

    def __init__(self, project):
        self.project = project

    @property
    def DSEntity(self):
        return AbstractDSEntity("DSEntity", (BaseEntity,), {'__kind__': None, '__project__': self.project})

    def save_multi(self, entities : List[BaseEntity], exclude_from_indexes=()):
        client = datastore.Client(project=self.project)
        [entity._save_offline(exclude_from_indexes=exclude_from_indexes) for entity in entities]
        client.put_multi([entity.get_raw_entity() for entity in entities])

    def delete_multi(self, entities : List[BaseEntity], batch_size: int = 500):
        client = datastore.Client(project=self.project)
        delete_keys = []
        for index, entity in enumerate(entities, 1):
            delete_keys.append(entity)
            if not index % batch_size:
                self._delete_batch(client, delete_keys)
                delete_keys = []
        self._delete_batch(client, delete_keys)

    def _delete_batch(self, client, keys: List[BaseEntity]):
        batch = client.batch()
        batch.begin()
        [batch.delete(key) for key in keys]
        batch.commit()

    def generate_key(self, kind: str, identifier: str, parent_key: Key = None):
        return datastore.Client(project=self.project).key(kind, identifier, parent=parent_key)
