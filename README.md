1. `npm install`
2. make a database called "loader_test" in your local postgres
3. run setup.sql in that db
4. `node server.js`
5. visit `localhost:4000`
6. `python loader.py myfile.ld.json` (a file which contains one JSON array of strings on each line)

The data should stream into the browser.