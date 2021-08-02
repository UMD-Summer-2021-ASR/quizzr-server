# Include several utility methods such as getting a route by operation ID.
from copy import deepcopy

import yaml


class QuizzrAPISpec:
    STUB = 1

    def __init__(self, api_path):
        with open(api_path) as api_f:
            self.api = yaml.load(api_f.read(), Loader=yaml.FullLoader)

    def path_for(self, op_id: str):
        """Retrieve the path and operation type associated with the given operation ID."""
        for path, ops in self.api["paths"].items():
            for op, description in ops.items():
                if type(description) is dict and description.get("operationId") == op_id:
                    return path, op

    def get_schema_stub(self, schema_name: str):
        """Return a copy of the stub example of a schema."""
        return deepcopy(self.api["components"]["schemas"][schema_name]["examples"][self.STUB])

    def get_schema(self, schema_name: str, resolve_references=False) -> dict:
        """
        Return a schema from the API specification, optionally with all references resolved.
        :param schema_name: The name of the schema as identified in the specification.
        :param resolve_references: If this is True, replace all references with their actual values.
        """
        schema = self.api["components"]["schemas"][schema_name]
        if resolve_references:
            return self.build_schema(schema)
        return schema

    def build_schema(self, in_schema: dict) -> dict:
        """Recursively resolve all references in a schema for validation. Does not support circular references"""
        # TODO: Multiple types
        out_schema = deepcopy(in_schema)
        if "$ref" in out_schema:
            out_schema.update(
                self.build_schema(
                    self.lookup_ref(
                        out_schema.pop("$ref")
                    )
                )
            )
        elif "oneOf" in out_schema:
            for i, value in enumerate(out_schema["oneOf"]):
                out_schema["oneOf"][i] = self.build_schema(value)
        elif "allOf" in out_schema:
            for i, value in enumerate(out_schema["allOf"]):
                out_schema["allOf"][i] = self.build_schema(value)
        elif "anyOf" in out_schema:
            for i, value in enumerate(out_schema["anyOf"]):
                out_schema["anyOf"][i] = self.build_schema(value)
        elif out_schema["type"] == "object":
            for prop, value in out_schema["properties"].items():
                out_schema["properties"][prop] = self.build_schema(value)
        elif out_schema["type"] == "array":
            out_schema["items"] = self.build_schema(out_schema["items"])

        return out_schema

    def lookup_ref(self, path: str):
        """
        Return the value of the object at the specified path.

        :param path: A slash-delimited string starting with # that defines the path to the schema.
        :return: The result of the lookup
        """
        if not path.startswith("#/"):
            raise ValueError(f"Path '{path}' is not absolute")
        layers = path.split("/")[1:]  # Exclude "#/"
        root = self.api
        while layers:
            root = root[layers.pop(0)]
        return root
