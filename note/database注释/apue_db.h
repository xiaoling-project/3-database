#ifndef _APUE_DB_H  /* 保证只包含该头文件一次 */
#define _APUE_DB_H

/**************apue.h文件的部分内容***************/


#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <fcntl.h>
#include <unistd.h>

#include <sys/types.h>
#include <sys/stat.h>
#include <stddef.h>
#include <error.h>
#include <errno.h>

#include <sys/uio.h>
#include <stdarg.h>


#define MAXLINE 4096//后面出错处理哪里用到
#define FILE_MODE (S_IRUSR|S_IWUSR|S_IRGRP|S_IROTH) //文件权限   （文件所有者）读|写| （文件组）读|写

//通过宏定义简化锁的操作
#define writew_lock(fd,offset,whence,len) 	lock_reg((fd),F_SETLKW,F_WRLCK,(offset),(whence),(len)) //F_SETLKW如果已经加锁进程挂起等待
#define readw_lock(fd,offset,whence,len)  	lock_reg((fd),F_SETLKW,F_RDLCK,(offset),(whence),(len))	//F_SETLK如果已经加锁直接返回
#define un_lock(fd,offset,whence,len)		lock_reg((fd),F_SETLK,F_UNLCK,(offset),(whence),(len))//F_UNLCK解锁  F_RDLCK加读锁 F_WRLCK加写锁
//补充一个知识点：如果进程已经加锁再加锁新锁会替代旧锁





//给文件加记录锁,以上定义的三个宏调用此函数
int lock_reg(int fd, int cmd, int type, off_t offset, int whence, off_t len);
/*int lock_reg(int fd, int cmd, int type, off_t offset, int whence, off_t len) {
	struct flock lock;
	lock.l_len = len;
	lock.l_start = offset;
	lock.l_type = type;
    lock.l_whence = whence;
	return (fcntl(fd, cmd, &lock));
}
放在这里只是方便你们看实际在db.c*/ 



//对不同的错误，由不同的报错处理，err_doit对外屏蔽，看db.c即可
void err_doit(int errnoflag, int error, const char * fmt, va_list ap);
void err_dump(const char *fmt, ...) ;
void err_sys(const char * fmt,...) ;   
void err_quit(const char * fmt ,...);


/************************************************/


typedef void * DBHANDLE;  //数据库的引用

DBHANDLE db_open(const char *pathname, int oflag, ...);
void     db_close(DBHANDLE h);
char    *db_fetch(DBHANDLE h, const char *key);
int      db_store(DBHANDLE h, const char *key, const char *data, int flag);  //用户调用的函数
int      db_delete(DBHANDLE h, const char *key);
void     db_rewind(DBHANDLE h);
char    *db_nextrec(DBHANDLE h, char *key);

/* db_store : oflag */
#define DB_INSERT  1  /* insert new record only */
#define DB_REPLACE 2  /* replace existing record */
#define DB_STORE   3  /* replace or insert */

/* 实现的基本限制，为支持更大的数据库可更改这些限制 */
#define IDXLEN_MIN 6     //key长为1时的索引长度(即最小索引长度)
#define IDXLEN_MAX 1024  //实际索引最大长度
#define DATLEN_MIN 2     //数据至少包含一个字母和换行符
#define DATLEN_MAX 1024  //数据最大长

#endif  /* _APUE_DB_H */
