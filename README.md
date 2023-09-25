# FX 1 Minute Data to PostgrSQL
A .env based python script to take the data scraped via https://github.com/philipperemy/FX-1-Minute-Data
and parse it for very quick use into a postgresql database. In it's current form all tick types are averaged
into a single scalar value that should work for the most simple of applications but modification could be made
to support full pair data storage.
