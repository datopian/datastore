"""Auth providers — pluggable authentication/authorization backends.

One subpackage per provider (`ckan/`, `jwt/`, `anonymous/`); each exports
`Provider = <ConcreteClass>` so the registry can `importlib.import_module`
it without listing names statically. Add a new provider by dropping a
sibling folder with the same layout.
"""
