const _ = require('lodash');
const { Pool } = require('pg');
const express = require('express');
const bodyParser = require('body-parser');

const app = express();

const PORT = process.env.PORT || 3000;

app.use(bodyParser.json());
app.use(
  bodyParser.urlencoded({
    extended: true,
  }),
);

const names = [
  'Carlos Clarke',
  'Ellen Holmes',
  'Bentley Villarreal',
  'Bryanna Ramirez',
  'Glenn Cuevas',
  'Yair Woodard',
  'Kayley Pollard',
  'Angela Valencia',
  'Annalise Fowler',
  'Heidy Warren',
  'Karter Li',
  'Cameron Curry',
  'Baylee Wood',
  'Kayley Richardson',
  'Ronnie Peterson',
  'Elisabeth Osborn',
  'Zack Tran',
  'Sanai Osborne',
  'Katie Perez',
  'Dalia Lynch',
  'Mara Travis',
  'Kaitlyn Waller',
  'Will Kaufman',
  'Hezekiah Watts',
  'Kamila Duke',
  'Zayden Gallegos',
  'Sandra Terrell',
  'Jaeden Santiago',
  'Sophie Gardner',
  'Brycen Choi',
  'Emilee Roberts',
  'Jorden Manning',
  'Daphne Russo',
  'Adalyn Craig',
  'Jace Huber',
  'Ireland Gordon',
  'Judah Terry',
  'Sloane Noble',
  'Helen Schwartz',
  'Melvin Cole',
  'Danny Frye',
  'Mary Curtis',
  'Gia Mullen',
  'Landen Vega',
  'Addison Leblanc',
  'Jaiden Francis',
  'Marlene Galvan',
  'Dakota Wells',
  'Devin Huber',
  'Jaxson Erickson',
  'Jessica Burnett',
  'Emerson Johnson',
  'Gianni Maldonado',
  'Kale Morales',
  'Alexia Brady',
  'Arnav Solis',
  'Blaze Henson',
  'Kamora Clay',
  'Maleah Mcneil',
  'Travis Macdonald',
  'Arianna Zavala',
  'Efrain Mccullough',
  'Kaylyn Hensley',
  'Anabel Strong',
  'Haylie Baldwin',
  'Gretchen Haney',
  'Jagger Silva',
  'Rocco Drake',
  'Giovanna Clayton',
  'Justine Giles',
  'Lia Forbes',
  'Leland Frazier',
  'Reynaldo Steele',
  'Baylee Soto',
  'Cody Beck',
  'Ricardo Singleton',
  'Evelin Bond',
  'Ahmed Cole',
  'Ryker Knox',
  'Marina Pena',
  'Aniyah Petty',
  'Colby Hull',
  'Alanna Mosley',
  'Marisol Hughes',
  'Easton Golden',
  'Ismael Cooley',
  'Michael Green',
  'Maribel Cooper',
  'Isis Soto',
  'Travis Glass',
  'Payten Dixon',
  'Lizeth Sullivan',
  'Braylon Herrera',
  'Journey Collier',
  'Madalynn Long',
  'Mark Goodman',
  'Jakob Pena',
  'Josiah Montes',
  'Cristina Rivas',
  'Lea Reid',
];
const emails = names.map((name) => {
  name = name.toLowerCase();
  name = name.replace(/\s/g, '_');
  return `${name}@example.com`;
});

function getPool(host, port, database, user, password, ca) {
  return new Pool({
    ssl: { ca },
    user,
    host,
    database,
    password,
    port,
  });
}

function insertQuery(pool, name, email) {
  return pool.query('INSERT INTO users (name, email) VALUES ($1, $2)', [
    name,
    email,
  ]);
}

function insertRandomQuery(pool) {
  const i = Math.trunc(Math.random() * names.length);
  return insertQuery(pool, names[i], emails[i]);
}

async function loadTestInserts(pool, runForMs = 3000, reporter = console.log) {
  const startTime = +new Date();
  let total = 0;

  const genResults = () => {
    const duration = +new Date() - startTime;
    const rate = (total * 1000) / duration;
    return {
      duration,
      total,
      rate,
    };
  };

  const handle = setInterval(() => reporter(genResults()), 1000);

  while (+new Date() < startTime + runForMs) {
    const p = [];
    for (let i = 0; i < 100; i++) {
      p.push(insertRandomQuery(pool));
    }
    await Promise.all(p);
    total += p.length;
  }

  clearInterval(handle);

  return genResults();
}

async function closeQuery(pool) {
  await pool.end();
}

async function runLoadTest(pool, parallelTracks, runForMs) {
  const p = [];
  const totals = [];
  const startTime = +new Date();
  const genResults = () => {
    const duration = +new Date() - startTime;
    const total = totals.reduce((a, c) => a + c);
    const rate = (total * 1000) / duration;
    return {
      duration,
      total,
      rate,
    };
  };

  const handle = setInterval(() => console.log(genResults()), 1000);

  for (let i = 0; i < parallelTracks; i++) {
    totals.push(0);
    p.push(
      loadTestInserts(pool, runForMs, (results) => {
        console.log(`[${i}] ${JSON.stringify(results)}`);
        totals[i] = results.total;
      }),
    );
  }

  clearInterval(handle);

  await Promise.all(p);

  return genResults();
}

app.get('/', async (req, res) => {
  console.log(Object.keys(req));
  const {
    host,
    port,
    database,
    user,
    password,
    ca,
    parallelTracks,
    runForMs,
  } = req.query;
  const targetDuration = parseInt(runForMs);
  const pool = getPool(host, port, database, user, password, ca);
  const results = await runLoadTest(pool, parallelTracks, targetDuration);
  closeQuery(pool);
  res.send(
    JSON.stringify({
      host,
      port,
      database,
      user,
      password,
      ca,
      parallelTracks,
      runForMs,
      results,
    }),
  );
});

app.listen(PORT, () => {
  console.log(`App running on port ${PORT}.`);
});
