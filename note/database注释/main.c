
//#include "apue.h"
#include "apue_db.h"

int main(void)
{
	DBHANDLE db;

	if ((db = db_open("db4", O_RDWR | O_CREAT | O_TRUNC, FILE_MODE)) == NULL )   //参看书中的解释
		err_sys("db_open error");

/*	if (db_store(db, "Alpah", "data1", DB_INSERT) != 0)
		err_quit("db_store error for alpha");
	if (db_store(db, "beta", "Data for beta", DB_INSERT) != 0)
		err_quit("db_store error for beta");*/
	if (db_store(db, "sss", "hhhh", DB_INSERT) != 0)
		err_quit("db_store error for gamma");

	db_close(db);
	exit(0);
}
