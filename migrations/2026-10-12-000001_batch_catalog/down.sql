DROP TRIGGER task_catalog_delete ON task;
DROP TRIGGER task_catalog_insert ON task;
DROP FUNCTION catalog_deleted_tasks();
DROP FUNCTION catalog_inserted_tasks();
DROP TABLE batch_catalog;
