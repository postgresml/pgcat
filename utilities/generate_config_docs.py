import re
import tomli

class DocGenerator:
    def __init__(self, filename):
        self.doc = []
        self.current_section = ""
        self.current_comment = []
        self.current_field_name = ""
        self.current_field_value = []
        self.current_field_unset = False
        self.filename = filename

    def write(self):
        with open("../CONFIG.md", "w") as text_file:
            text_file.write("# PgCat Configurations \n")
            for entry in self.doc:
                if entry["name"] == "__section__":
                    text_file.write("## `" + entry["section"] + "` Section" + "\n")
                    text_file.write("\n")
                    continue
                text_file.write("### " + entry["name"]+ "\n")
                text_file.write("```"+ "\n")
                text_file.write("path: " + entry["fqdn"]+ "\n")
                text_file.write("default: " + entry["defaults"].strip()+ "\n")
                if entry["example"] is not None:
                    text_file.write("example: " + entry["example"].strip()+ "\n")
                text_file.write("```"+ "\n")
                text_file.write("\n")
                text_file.write(entry["comment"]+ "\n")
                text_file.write("\n")

    def save_entry(self):
        if len(self.current_field_name) == 0:
            return
        if len(self.current_comment) == 0:
            return
        self.current_section = self.current_section.replace("sharded_db", "<pool_name>")
        self.current_section = self.current_section.replace("simple_db", "<pool_name>")
        self.current_section = self.current_section.replace("users.0", "users.<user_index>")
        self.current_section = self.current_section.replace("users.1", "users.<user_index>")
        self.current_section = self.current_section.replace("shards.0", "shards.<shard_index>")
        self.current_section = self.current_section.replace("shards.1", "shards.<shard_index>")
        self.doc.append(
            {
                "name": self.current_field_name,
                "fqdn": self.current_section + "." + self.current_field_name,
                "section": self.current_section,
                "comment": "\n".join(self.current_comment),
                "defaults": self.current_field_value if not self.current_field_unset else "<UNSET>",
                "example": self.current_field_value  if self.current_field_unset  else None
            }
        )
        self.current_comment = []
        self.current_field_name = ""
        self.current_field_value = []
    def parse(self):
        with open("../pgcat.toml", "r") as f:
            for line in f.readlines():
                line = line.strip()
                if len(line) == 0:
                    self.save_entry()

                if line.startswith("["):
                    self.current_section = line[1:-1]
                    self.current_field_name = "__section__"
                    self.current_field_unset = False
                    self.save_entry()

                elif line.startswith("#"):
                    results = re.search("^#\s*([A-Za-z0-9_]+)\s*=(.+)$", line)
                    if results is not None:
                        self.current_field_name = results.group(1)
                        self.current_field_value = results.group(2)
                        self.current_field_unset = True
                        self.save_entry()
                    else:
                        self.current_comment.append(line[1:].strip())
                else:
                    results = re.search("^\s*([A-Za-z0-9_]+)\s*=(.+)$", line)
                    if results is None:
                        continue
                    self.current_field_name = results.group(1)
                    self.current_field_value = results.group(2)
                    self.current_field_unset = False
                    self.save_entry()
        self.save_entry()
        return self


DocGenerator("../pgcat.toml").parse().write()

