set -e

echo "Attente de MinIO..."
until mc alias set local http://minio:9000 "${MINIO_ROOT_USER}" "${MINIO_ROOT_PASSWORD}" 2>/dev/null; do
    sleep 2
done

mc mb local/data-lake --ignore-existing
echo "Bucket data-lake cree"