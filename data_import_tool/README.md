# Steps to create distributable
### Install Pyinstaller
`pip install pyinstaller`

### Generate release spec file
`pyinstaller --onefile --name import_tool import.py`
### edit import_tool.spec file to include import_config.json file
`datas=[('import_config.json', '.')],`
### Generate program with 
`pyinstaller import_tool.spec` 