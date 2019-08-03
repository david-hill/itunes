#!/bin/bash

yum install mariadb-server python2-PyMySQL.noarch python2-mysql
pip install musicbrainzngs


mysql -e "create database musicbrainz;"
mysql -D musicbrainz -e "CREATE TABLE `musicbrainz` ( `present` int(11) DEFAULT NULL, `artist` varchar(100) DEFAULT NULL, `name` varchar(250) DEFAULT NULL, `type` varchar(30) DEFAULT NULL, `year` varchar(4) DEFAULT NULL, `last_updated` timestamp NULL DEFAULT NULL) ENGINE=MyISAM DEFAULT CHARSET=utf8"
mysql -D musicbrainz "ALTER DATABASE musicbrainz CHARACTER SET utf8 COLLATE utf8_general_ci;"
