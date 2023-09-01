CREATE TABLE IF NOT EXISTS main.top_movies(
	id INT not null,
    genre_ids text,
    title varchar(255),
    overview text,
    popularity float,
    release_date DATE,
    vote_average float,
    vote_count INT,
    PRIMARY key (id)
);

TRUNCATE TABLE main.top_movies;

INSERT INTO main.top_movies
	SELECT
    `load_top_movies`.`id`,
    `load_top_movies`.`genre_ids`,
    `load_top_movies`.`title`,
    `load_top_movies`.`overview`,
    `load_top_movies`.`popularity`,
    `load_top_movies`.`release_date`,
    `load_top_movies`.`vote_average`,
    `load_top_movies`.`vote_count`
FROM `etl`.`load_top_movies`;
