const express = require('express');
const router = express.Router();

/* GET home page. */
router.get('/', function(req, res, next) {
  res.render('index', { title: 'Finanzkraft, 2022 by Anton Schegg' });
});

module.exports = router;
