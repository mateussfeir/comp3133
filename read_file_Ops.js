const fs = require("fs");
const path = require("path");
const csv = require("csv-parser");

const inputFile = path.join(__dirname, "input_countries.csv");
const canadaFile = path.join(__dirname, "canada.txt");
const usaFile = path.join(__dirname, "usa.txt");

// a) delete canada.txt and usa.txt if already exist
[canadaFile, usaFile].forEach((file) => {
  if (fs.existsSync(file)) {
    fs.unlinkSync(file);
    console.log(`Deleted: ${path.basename(file)}`);
  }
});

// streams to write output
const canadaStream = fs.createWriteStream(canadaFile, { flags: "a" });
const usaStream = fs.createWriteStream(usaFile, { flags: "a" });

// header (as required sample)
const header = "country,year,population\n";
canadaStream.write(header);
usaStream.write(header);

// read CSV using stream and filter
fs.createReadStream(inputFile)
  .pipe(csv())
  .on("data", (row) => {
    const country = (row.country || "").trim().toLowerCase();
    const year = (row.year || "").trim();
    const population = (row.population || "").trim();

    if (!country || !year || !population) return;

    if (country === "canada") {
      canadaStream.write(`${country},${year},${population}\n`);
    } else if (country === "united states") {
      usaStream.write(`${country},${year},${population}\n`);
    }
  })
  .on("end", () => {
    canadaStream.end();
    usaStream.end();
    console.log("Done! Created canada.txt and usa.txt");
  })
  .on("error", (err) => {
    console.error("Error reading CSV:", err.message);
  });
