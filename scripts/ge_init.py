import great_expectations as gx

context = gx.get_context(mode="file", project_root_dir=".")
print(f"GX context initialized at: {context.root_directory}")
