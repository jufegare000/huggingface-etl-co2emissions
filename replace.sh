#!/usr/bin/env bash
set -euo pipefail

TARGET_DIR="iac/environments/dev"

if [ ! -d "$TARGET_DIR" ]; then
  echo "Error: no existe el directorio $TARGET_DIR"
  exit 1
fi

echo "Reemplazando var.project_name por local.project_name en $TARGET_DIR..."

find "$TARGET_DIR" -type f -name "*.tf" -print0 \
  | xargs -0 sed -i.bak 's/var\.project_name/local.project_name/g'

echo "Backups creados con extensión .bak"

echo "Validando referencias restantes..."
if grep -R "var.project_name" "$TARGET_DIR"; then
  echo "Aún quedan referencias a var.project_name."
  exit 1
else
  echo "No quedan referencias a var.project_name en $TARGET_DIR."
fi

echo "Archivos modificados:"
find "$TARGET_DIR" -type f -name "*.tf.bak" \
  | sed 's/\.bak$//'

echo "Listo."
