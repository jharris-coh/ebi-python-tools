class Table:
    def __init__(self, table_name: str):
        def remove_quotes(part: str) -> str:
            return part.replace('"', '').replace("[", '').replace(']', '')

        self.fqn = table_name.split(".")
        self.fqn_quoted = [f'"{remove_quotes(part)}"' for part in self.fqn]
        self.full_name = ".".join(self.fqn_quoted)
        self.table_name = self.fqn[-1]
        self.schema_name = self.fqn[-2] if len(self.fqn) >= 2 else None
        self.database_name = self.fqn[-3] if len(self.fqn) >= 3 else None
