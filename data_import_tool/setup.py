from cx_Freeze import setup, Executable


# Include additional files (e.g., config and SQL queries)
include_files = [
    ("import_config.json", "import_config.json") # Config file
]

# Dependencies required for your project
packages = ["pytds", "os", "json", "csv", "glob", "shutil", "hashlib", "zipfile", "logging"]



setup(
    name="ImportTool",
    version="1.0",
    description="Data import",
    options={
        "build_exe": {
            "packages": packages,
            "include_files": include_files,
            "build_exe": "release"      # Output folder
        }
    },
    executables=[Executable("import.py", base=None)]
)
