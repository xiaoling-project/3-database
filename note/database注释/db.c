//#include "apue.h" 
#include "apue_db.h"

/************************apue.c文件的部分内容**************************/
//记录锁，三个关于锁操作的宏调用此函数，对外屏蔽了flock
int lock_reg(int fd, int cmd, int type, off_t offset, int whence, off_t len) {
	struct flock lock;
	lock.l_len = len;
	lock.l_start = offset;
	lock.l_type = type;
    lock.l_whence = whence;
	return (fcntl(fd, cmd, &lock));
}


void err_doit(int errnoflag, int error, const char * fmt, va_list ap)
{																			//打印错误信息，后面都会调用这个函数
	char buf[MAXLINE];
	vsnprintf(buf, MAXLINE - 1, fmt, ap);                                               
	if (errnoflag)
		snprintf(buf + strlen(buf), MAXLINE - strlen(buf) - 1, ":%s", strerror(error));
	fflush(stdout);      
	fputs(buf, stderr);
	fflush(NULL);
}
                                            // 《vsnprintf()函数 & vfprintf()函数》 https://blog.csdn.net/qq_37824129/article/details/787632
											//《VA_LIST的用法》, 一起来围观吧 https://blog.csdn.net/pheror_abu/article/details/5340486   
											//  链接<strarg.h>   va_list后面还会用到
void err_dump(const char *fmt, ...) {
	va_list ap;
	va_start(ap, fmt);
	err_doit(1,errno,fmt,ap);                                  
	va_end(ap);										
	abort();//异常中止一个进程
	exit(1);
}

void err_sys(const char * fmt,...) {
	va_list  ap;
	va_start(ap, fmt);
	err_doit(1, errno, fmt, ap);
	va_end(ap);
	exit(1);
}

void err_quit(const char * fmt ,...){
    va_list ap;
    va_start(ap,fmt);
    err_doit(0,0,fmt,ap);		
    va_end(ap);
    exit(1);
}

/********************************************************************/



#define IDXLEN_SZ   4		//索引长度所占字节
#define SEP         ':'		//分隔符
#define SPACE       ' '		//删除一条数据时，用" "填充
#define NEWLINE     '\n'	//定义换行标志，为了方便查看文件内容，如果没有查看的需要可以不加换行符(方便more与cat等命令查看)

/* The following definitions are for hash chains and free list chain in the index file */
#define PTR_SZ      6     	//散列链的指针所占字节
#define PTR_MAX  999999   	//指针所能指向的最大范围,因为指针所占的字节为6，用ascall码表示数字所以最大也就6个9即999999
#define NHASH_DEF   137   	//散列表的大小，可以设置（用散列链表解决散列冲突时将hash值设置为质数可以最大的的让各链中的数据分布均匀）
#define FREE_OFF    0     	//空闲链表的位置
#define HASH_OFF PTR_SZ   	//散列表的位置（即空闲链表后）

typedef unsigned long DBHASH;  /* hash values */
typedef unsigned long COUNT;   /* unsigned counter */

/* 记录打开数据的所有信息, db_open函数返回DB结构的指针DBHANDLE值 */
typedef struct
{
	int   idxfd;      //索引文件的文件描述符
	int   datfd;      //数据文件的文件描述符
	char  *idxbuf;    //给索引内容分配的缓冲区，即先将索引记录写入缓冲再写入文件所用的缓冲区
	char  *datbuf;    //给数据内容分配的缓冲区，与上面类似
	char  *name;      //数据库名，

	off_t idxoff;     //当前索引记录的位置，key的位置= idxoff + PTR_SZ + IDXLEN_SZ
	size_t idxlen;    // 当前索引记录的长度，IDXLEN_SZ指定其所占字节
	off_t  datoff;	  //对应数据的位置在数据文件中的偏移量     offset in data file of data record
	size_t datlen;    //对应数据长，包含"\n"   length of data    record include newline at end
	off_t  ptrval;    //当前索引记录中指向下一个hash节点指针的值    contents of chain ptr in index record
	off_t  ptroff;    //指向当前索引记录的指针的值    chain ptr offset pointing to this record
	off_t  chainoff;  /*当前索引记录所在hash链头指针相对于文件开始处的偏移量     offset of hash chain for this index record */
	off_t  hashoff;   /* hash表相对于索引文件开始处的偏移量前面还有一个空闲链表的头指针    offset in index file of hash table */
	DBHASH nhash;     //当前散列表的大小

	/* 对成功和不成功的操作计数  */
	COUNT  cnt_delok;    /* delete OK */
	COUNT  cnt_delerr;   /* delete error */
	COUNT  cnt_fetchok;  /* fetch OK */
	COUNT  cnt_fetcherr; /* fetch error */
	COUNT  cnt_nextrec;  /* nextrec */
	COUNT  cnt_stor1;    /* store: DB_INSERT, no empty, appended */
	COUNT  cnt_stor2;    /* store: DB_INSERT, found empty, reused */
	COUNT  cnt_stor3;    /* store: DB_REPLACE, diff len, appended */
	COUNT  cnt_stor4;    /* store: DB_REPLACE, same len, overwrote */
	COUNT  cnt_storerr;  /* store error */
}DB;

/* 内部私有函数。声明为static，只有同一文件中的其他函数才能调用 */
static DB *_db_alloc(int);
static void _db_dodelete(DB *);
static int _db_find_and_lock(DB *, const char *, int);
static int _db_findfree(DB *, int, int);
static void _db_free(DB *);
static DBHASH _db_hash(DB *, const char *);
static char *_db_readdat(DB *);
static off_t _db_readidx(DB *, off_t);
static off_t _db_readptr(DB *, off_t);
static void _db_writedat(DB *, const char *, off_t, int);
static void _db_writeidx(DB *, const char *, off_t, int, off_t);
static void _db_writeptr(DB *, off_t, off_t);

/*
	_db_open
		根据数据库名，打开数据库的索引文件和数据文件，将其文件描述符存入DB结构
		如果oflag中包含O_CREAT,需要创建文件，第三个可选参数是文件的权限
*/
DBHANDLE db_open(const char *pathname, int oflag, ...)    //有可变参数的应用，查看前面的博客
{
	DB *db;
	int len, mode;
	size_t i; 
	char asciiptr[PTR_SZ + 1], hash[(NHASH_DEF + 1) * PTR_SZ + 2];/*aciiptr末尾是0，hash末尾是‘\n’和0  末尾需要加0，
																	变为字符串（后面要用到字符串函数就是以打印的方式将空闲链表以及hash表打印到索引文件上，hash表末尾还需要加换行符所以+2）
																	注意这里NHASH_DEF+1是因为包含了空闲链表将其一起初始化*/
	struct stat statbuff;                 //获取文件状态的缓冲区

	len = strlen(pathname);
	if ((db = _db_alloc(len)) == NULL)
		//err_dump定义在"apue.h"						//为DB*db分配空间
		err_dump("db_open: _db_alloc error for DB");

	db->nhash = NHASH_DEF;  //当前散列表的大小设置为137
	db->hashoff = HASH_OFF; //索引文件中散列表的偏移
	strcpy(db->name, pathname);
	strcat(db->name, ".idx");  //索引文件名，修改后缀即为数据库文件名

	if (oflag & O_CREAT)
	{//取第三个参数
		va_list ap;
		va_start(ap, oflag);					//可变参数的使用
		mode = va_arg(ap, int);          		
		va_end(ap);

		db->idxfd = open(db->name, oflag, mode);
		strcpy(db->name + len, ".dat");
		db->datfd = open(db->name, oflag, mode);
	}
	else				//没有O_CREATE直接打开已有文件，open只用两个参数
	{
		db->idxfd = open(db->name, oflag);
		strcpy(db->name + len, ".dat");
		db->datfd = open(db->name, oflag);
	}

	/* 如果打开或创建任意数据库文件时出错 */
	if (db->idxfd < 0 || db->datfd < 0)
	{
		_db_free(db);  /* 清除DB结构 */
		return (NULL);
	}

	if ((oflag & (O_CREAT | O_TRUNC)) == (O_CREAT | O_TRUNC))   	//如果建立数据库就加锁，看书中注释121-130
	{
		/*给索引文件加锁*/
		if (writew_lock(db->idxfd, 0, SEEK_SET, 0) < 0)
			err_dump("db_open: writew_lock error");

		/* 读取文件大小，如果为0，表示是本进程创建的文件，需要初始化索引文件，写入散列表 */
		if (fstat(db->idxfd, &statbuff) < 0)
			err_sys("db_open: fstat error");

		if (statbuff.st_size == 0)  //判断文件是不是刚开始创建的，看书中注释131-137
		{
			sprintf(asciiptr, "%*d", PTR_SZ, 0);/*	将"     0"写入asciiptr，这里的*相当于读取PTR_SZ的值作为%d的宽度(后面会多次用到)
													与scanf与sscanf那里的*不同那里是忽略掉读取的数据后面 
												*/
			hash[0] = 0;//0表示空字符，即字符串结尾，保证之后调用strcat不会出错
			for (i = 0; i < NHASH_DEF + 1; i++)   
				strcat(hash, asciiptr);
			strcat(hash, "\n");//hash表末尾还要加一个换行符"
			i = strlen(hash);
			if (write(db->idxfd, hash, i) != i)
				err_dump("db_open: index file init write error");
		}
		/* 解锁索引文件 */
		if (un_lock(db->idxfd, 0, SEEK_SET, 0) < 0)
			err_dump("db_open: un_lock error");
	}
	//将db->idxoff定位到第一条索引
	db_rewind(db);

	return (db);
}

//为DB及其成员(name,idxbuf,databuf)分配空间，初始化两个文件描述符
static DB *_db_alloc(int namelen)
{
	DB *db;

	if ((db = calloc(1, sizeof(DB))) == NULL)  //这里用calloc而不用malloc是因为前者会将将分配的内存空间的值都设置为0 
		err_dump("_db_alloc: calloc error for DB");
	db->idxfd = db->datfd = -1;    //文件描述符也被设为0，所以改为-1  参看书中注释152-164

	if ((db->name = malloc(namelen + 5)) == NULL)
		err_dump("_db_alloc: malloc error for name");  //加5的原因是因为后面需要添加.idx和一个0

	if ((db->idxbuf = malloc(IDXLEN_MAX + 2)) == NULL)      //+2是因为后面有时候需要添加一个换行符和0
		err_dump("_db_alloc: malloc error for index buffer");
	if ((db->datbuf = malloc(DATLEN_MAX + 2)) == NULL)	
		err_dump("_db_alloc: malloc error for data buffer");
	return (db);
}

/* Relinquish access to the database */
void db_close(DBHANDLE h)
{
	_db_free((DB *)h);  //对_db_free的包装，使其传入的参数为DBHANDLE
}

//在操作结束，或出错时调用free，释放空间
static void _db_free(DB *db)
{  
	/* 关闭文件 */
	if (db->idxfd >= 0)    //因为上面把文件描述符置为-1所以如果没有正确打开文件就不用关闭  
		close(db->idxfd);
	if (db->datfd >= 0)
		close(db->datfd);
	/* 释放动态分配的缓冲 */
	if (db->idxbuf != NULL)
		free(db->idxbuf);
	if (db->datbuf != NULL)
		free(db->datbuf);
	if (db->name != NULL)
		free(db->name);
	/* 释放DB结构占用的存储区 */
	free(db);
}


/* 根据给定的键读取一条数据 */
char *db_fetch(DBHANDLE h, const char *key)
{
	DB *db = h;
	char *ptr;

	/* 查找该记录，若查找成功将结果存入DB结构 */
	if (_db_find_and_lock(db, key, 0) < 0)//此过程给索引文件对应的散列链加锁
	{
		ptr = NULL;
		db->cnt_fetcherr++;
	}
	else  /* 成功 */
	{
		ptr = _db_readdat(db);  //读数据文件中的数据 
		db->cnt_fetchok++;
	}

	if (un_lock(db->idxfd, db->chainoff, SEEK_SET, 1) < 0)//解锁
		err_dump("db_fetch: un_lock error");
	return (ptr);
}

/* 在函数库内部按给定的键查找记录 */
/* 如果想在索引文件上加一把写锁，则将writelock参数设置为非0；否则加读锁 */
static int _db_find_and_lock(DB *db, const char *key, int writelock)
{
	off_t offset, nextoffset;

	/*根据关键字生成hash值，计算出散列链的位置*/
	db->chainoff = (_db_hash(db, key) * PTR_SZ) + db->hashoff;
	db->ptroff = db->chainoff;

	/* 只锁该散列链开始处的第1个字节,允许多个进程访问索引文件的不同散列链 */
	if (writelock)//加了写锁
	{
		if (writew_lock(db->idxfd, db->chainoff, SEEK_SET, 1) < 0)
			err_dump("_db_find_and_lock: writew_lock error");
	}
	else
	{
		if (readw_lock(db->idxfd, db->chainoff, SEEK_SET, 1) < 0)
			err_dump("_db_find_and_lock: readw_lock error");
	}

	/* 读散列链中的第一个指针, 如果返回0，该散列链为空 */
	offset = _db_readptr(db, db->ptroff);//返回散列链的头指针，不存在返回0
	while (offset != 0)
	{
		//遍历链表，查找和key匹配的索引记录
		nextoffset = _db_readidx(db, offset);
		if (strcmp(db->idxbuf, key) == 0)
			break;  /* found a match */
		db->ptroff = offset;  /* offset of this (unequal) record   如果没找到就执行这一句使得ptroff值为前索引记录地址否则就为上一条索引记录地址*/
		offset = nextoffset;  /* next one to compare */
	}

	//达到链表尾时，如果匹配，直接退出，offset=该索引偏移  参照书中注释269-273  其中datlen和datoff在_db_readidx函数中赋值
	return (offset == 0 ? -1 : 0);
}

/* 根据给定的键计算散列值 */
static DBHASH _db_hash(DB *db, const char *key)
{
	//key的每一个字符的ascii码*(index+1)的和，再模nhash
	//nhash是素数,可以最大的使各散列表中的数据分布均匀
	DBHASH hval = 0;
	char c;
	int i;
	for (i = 1; (c = *key++) != 0; i++)                        //此数据库中所用的hash函数
		hval += c * i;
	return (hval % db->nhash);
}

/*
	读取一个指针，即读取offset位置的6个字符，并返回长整形
*/
static off_t _db_readptr(DB *db, off_t offset)  //参看注释287-302
{
	char asciiptr[PTR_SZ + 1];
	if (lseek(db->idxfd, offset, SEEK_SET) == -1)
		err_dump("_db_readptr: lseek error to ptr field");
	if (read(db->idxfd, asciiptr, PTR_SZ) != PTR_SZ)
		err_dump("_db_readptr: read error of ptr field");
	asciiptr[PTR_SZ] = 0;     //null terminate 构成字符串调用atol函数将字符串转变为长整型整数
	
	return (atol(asciiptr));
}

/*
	读取offset位置一条索引记录，并将索引信息填入DB结构
idxoff：当前索引记录相对于索引文件起始处的偏移量
ptrval：在散列链表中下一索引记录相对于索引文件起始处的偏移量
idxlen：当前索引记录的长度
idxbuf：实际索引记录
datoff：数据文件中该记录的偏移量
datlen：该数据记录的长度
*/
static off_t _db_readidx(DB *db, off_t offset)
{
	ssize_t i;													
	char *ptr1, *ptr2;
	char asciiptr[PTR_SZ + 1], asciilen[IDXLEN_SZ + 1];		//《struct iovec 结构体定义与使用》 https://blog.csdn.net/lixiaogang_theanswer/article/details/73385643

	struct iovec iov[2];

	/*
		传入的offset为0则从当前位置开始读
		因为索引记录应在散列表之后，offset不可能为0
	*/
	if ((db->idxoff = lseek(db->idxfd, offset, offset == 0 ?
		SEEK_CUR : SEEK_SET)) == -1)
		err_dump("_db_readidx: lseek error");

	/*
	调用readv读在索引记录开始处的两个定长字段：指向下一条索引记录的链表指针
	和该索引记录余下部分的长度（余下部分是不定长的）
	*/
	iov[0].iov_base = asciiptr;
	iov[0].iov_len = PTR_SZ;
	iov[1].iov_base = asciilen;
	iov[1].iov_len = IDXLEN_SZ;
	if ((i = readv(db->idxfd, &iov[0], 2)) != PTR_SZ + IDXLEN_SZ)
	{
		if (i == 0 && offset == 0)
			return (-1);  /* EOF for db_nextrec */
		err_dump("_db_readidx: readv error of index record");
	}

	/* This is our return value; always >= 0 */
	asciiptr[PTR_SZ] = 0;         /* null terminate */
	db->ptrval = atol(asciiptr);  /* offset of next key in chain */

	asciilen[IDXLEN_SZ] = 0;      /* null terminate */
	if ((db->idxlen = atoi(asciilen)) < IDXLEN_MIN ||
		db->idxlen > IDXLEN_MAX)
		err_dump("_db_readidx: invalid length");

	/* 将索引记录的不定长部分读入DB的idxbuf字段 */
	if ((i = read(db->idxfd, db->idxbuf, db->idxlen)) != db->idxlen)
		err_dump("_db_readidx: read error of index record");
	if (db->idxbuf[db->idxlen - 1] != NEWLINE)  /* 该记录应以换行符结束 */
		err_dump("_db_readidx: missing newline");
	db->idxbuf[db->idxlen - 1] = 0;  /* 将换行符替换为NULL */

	/* 索引记录划分为三个字段：键、对应数据记录的偏移量和数据记录的长度 */
	if ((ptr1 = strchr(db->idxbuf, SEP)) == NULL) //strchr(char* x,char m) 返回m第一次在x中出现时的指针
		err_dump("_db_readidx: missing first separator");
	*ptr1++ = 0;  /* replace SEP with null */	//结束后ptr1指向datoffset起始位置

	if ((ptr2 = strchr(ptr1, SEP)) == NULL)
		err_dump("_db_readidx: missing second separator");
	*ptr2++ = 0;								//结束后ptr2指向datlen起始位置

	if (strchr(ptr2, SEP) != NULL)
		err_dump("_db_readidx: too many separators");

	/* 将数据记录的偏移量和长度变为整型 */
	if ((db->datoff = atol(ptr1)) < 0)
		err_dump("_db_readidx: starting offset < 0");
	if ((db->datlen = atol(ptr2)) <= 0 || db->datlen > DATLEN_MAX)
		err_dump("_db_readidx: invalid length");

	return (db->ptrval);   /* 散列链中的下一条记录的偏移量 */
}

/* 根据db->datoff，db->datlen,读取一条数据 */
static char *_db_readdat(DB *db)
{
	if (lseek(db->datfd, db->datoff, SEEK_SET) == -1)
		err_dump("_db_readdat: lseek error");
	if (read(db->datfd, db->datbuf, db->datlen) != db->datlen)
		err_dump("_db_readdat: read error");
	if (db->datbuf[db->datlen - 1] != NEWLINE)
		err_dump("_db_readdat: missing newline");
	db->datbuf[db->datlen - 1] = 0;
	return (db->datbuf);
}

/* 
	删除与给定键匹配的一条记录 
*/
int db_delete(DBHANDLE h, const char *key)
{
	DB *db = h;
	int rc = 0;  //状态码，返回函数的执行状态

	//判断是否存在该键值
	if (_db_find_and_lock(db, key, 1) == 0)
	{//如果找到执行delete
		_db_dodelete(db);
		db->cnt_delok++;
	}
	else
	{
		rc = -1;
		db->cnt_delerr++;
	}
	//解除find_and_lock加的写锁
	if (un_lock(db->idxfd, db->chainoff, SEEK_SET, 1) < 0)
		err_dump("db_delete: un_lock error");
	return (rc);
}

/*
	实现删除函数，删除是指用宏SPACE填充，对于索引，填充键
	需要的操作：删除对应数据、删除对应索引（只删除key）、在空闲链表中添加已删除索引
*/
static void _db_dodelete(DB *db) //参照书中注释  441-461
{
	int i;
	char *ptr;
	off_t freeptr, saveptr;

	/* Set data buffer and key to all blanks */
	for (ptr = db->datbuf, i = 0; i < db->datlen - 1; i++)
		*ptr++ = SPACE;
	*ptr = 0;
	ptr = db->idxbuf;
	while (*ptr)
		*ptr++ = SPACE;

	
	/* We have to lock the free list */
	if (writew_lock(db->idxfd, FREE_OFF, SEEK_SET, 1) < 0)
		err_dump("_db_dodelete: writew_lock error");

	/* Write the data record with all blanks */
	_db_writedat(db, db->datbuf, db->datoff, SEEK_SET);

	/* 修改空闲链表 */
	freeptr = _db_readptr(db, FREE_OFF);

	/* 被_db_writeidx修改之前先保存散列链中的当前记录 */
	saveptr = db->ptrval;

	/* 用被删除的索引记录的偏移量更新空闲链表指针,
		   也就使其指向当前删除的这条记录, 从而将该被删除记录加到了空闲链表之首 */
	_db_writeidx(db, db->idxbuf, db->idxoff, SEEK_SET, freeptr);

	/* write the new free list pointer */
	_db_writeptr(db, FREE_OFF, db->idxoff);

	/* 修改散列链中前一条记录的指针，使其指向正删除记录之后的一条记录，
	   这样便从散列链中撤除了要删除的记录 */
	_db_writeptr(db, db->ptroff, saveptr);

	/* 对空闲链表解锁 */
	if (un_lock(db->idxfd, FREE_OFF, SEEK_SET, 1) < 0)
		err_dump("_db_dodelete: un_lock error");
}

/* 写一条数据或者从某一数据处开始写即覆盖改数据 */
static void _db_writedat(DB *db, const char *data, off_t offset, int whence)
{
	struct iovec iov[2];
	static char newline = NEWLINE;

	/* If we're appending, we have to lock before dong the lseek and
	   write to make the two an atomic operation. If we're overwriting
	   an existing record, we don't have to lock */
	//如果是从末尾写说明是向末尾添加数据操作，此时需要加锁，而其他操作由调用者加锁
	if (whence == SEEK_END)
		if (writew_lock(db->datfd, 0, SEEK_SET, 0) < 0)
			err_dump("_db_writedat: writew_lock error");

	if ((db->datoff = lseek(db->datfd, offset, whence)) == -1)
		err_dump("_db_writedat: lseek error");
	db->datlen = strlen(data) + 1;  /* datlen includes new line */

	/* 
		不能想当然地认为调用者缓冲区尾端有空间可以加换行符，
		所以先将换行符送入另一个缓冲，然后再从该缓冲写到数据记录
		(书中这句话应该是指可能在调用这函数之前程序员出错使得datbuf的空间不够，因为给datbuf分配空间时是malloc(DATLEN_MAX+2)理应是
		放得下一个换行符的，但为了避免出错这个函数传入了一个const char* data,保证数据不被破坏，然后为了避免两次系统调用所以用了writev，
		当然你新建一个char数组复制data再加一个换行符再调用write也是可行的)
	*/
	iov[0].iov_base = (char *)data;
	iov[0].iov_len = db->datlen - 1;
	iov[1].iov_base = &newline;
	iov[1].iov_len = 1;
	if (writev(db->datfd, &iov[0], 2) != db->datlen)
		err_dump("_db_writedat: writev error of dat record");

	if (whence == SEEK_END)
		if (un_lock(db->datfd, 0, SEEK_SET, 0) < 0)
			err_dump("_db_writedat: un_lock error");
}

/* 
	写一条索引，可以向末尾添加，也可以覆盖
	在设置DB中的datoff和datlen之前调用
   _db_writedat is called before this function to set datoff and datlen
   fields in the DB structure, which we need to write the index record  
*/
static void _db_writeidx(DB *db, const char *key, off_t offset,
	int whence, off_t ptrval)
{
	struct iovec iov[2];
	char asciiptrlen[PTR_SZ + IDXLEN_SZ + 1];
	int len;
	char *fmt;

	//检查ptrval是否合法
	if ((db->ptrval = ptrval) < 0 || ptrval > PTR_MAX)
		err_quit("_dbwriteidx: invalid ptr: %d", ptrval);
	if (sizeof(off_t) == sizeof(long long))//根据不同平台选择不同的数据类型   参见书中注释502-524
		fmt = "%s%c%lld%c%d\n";                           //换行符已经包括在fmt里面后面就不用加了
	else
		fmt = "%s%c%ld%c%d\n";
	sprintf(db->idxbuf, fmt, key, SEP, db->datoff, SEP, db->datlen);//将索引内容写入idxbuf
	if ((len = strlen(db->idxbuf)) < IDXLEN_MIN || len > IDXLEN_MAX)
		err_dump("_db_writeidx: invalid length");
	sprintf(asciiptrlen, "%*ld%*d", PTR_SZ, ptrval, IDXLEN_SZ, len);//将索引头部（也就是指向下一条记录的指针以及当前索引记录的长度）写入asciiptrlen

	/* If we're appending, we have to lock before dong the lseek
	   and write to make the two an atomic operation */
	if (whence == SEEK_END)//如果是添加数据，加锁，其他操作由调用者加锁
		if (writew_lock(db->idxfd, ((db->nhash + 1)*PTR_SZ) + 1,
			SEEK_SET, 0) < 0)
			err_dump("_db_writeidx: writew_lock error");

	/* 设置索引文件偏移量，从此处开始写索引记录，
		   将该偏移量存入DB结构的idxoff字段 */
	if ((db->idxoff = lseek(db->idxfd, offset, whence)) == -1)
		err_dump("_db_writeidx: lseek error");

	iov[0].iov_base = asciiptrlen;
	iov[0].iov_len = PTR_SZ + IDXLEN_SZ;
	iov[1].iov_base = db->idxbuf;
	iov[1].iov_len = len;
	if (writev(db->idxfd, &iov[0], 2) != PTR_SZ + IDXLEN_SZ + len)
		err_dump("_db_writeidx: writev error of index record");

	if (whence == SEEK_END)
		if (un_lock(db->idxfd, ((db->nhash + 1)*PTR_SZ) + 1,
			SEEK_SET, 0) < 0)
			err_dump("_db_writeidx: un_lock error");

}

/* 修改offset位置链表指针，ptrval表示要写入的数据 */
static void _db_writeptr(DB *db, off_t offset, off_t ptrval)
{
	char asciiptr[PTR_SZ + 1];

	/* 检查ptrval是否合法，然后转换为字符串 */
	if (ptrval < 0 || ptrval > PTR_MAX)
		err_quit("_db_writeptr: invalid ptr: %d", ptrval);
	sprintf(asciiptr, "%*ld", PTR_SZ, ptrval);

	/* 根据offset定位，将ptrval写入 */
	if (lseek(db->idxfd, offset, SEEK_SET) == -1)
		err_dump("_db_writeptr: lseek error to ptr field");
	if (write(db->idxfd, asciiptr, PTR_SZ) != PTR_SZ)
		err_dump("_db_writeptr: write error of ptr field");
}

/* 
	修改数据 
		DB_INSERT、DB_REPLACE、DB_STORE
*/
int db_store(DBHANDLE h, const char *key, const char *data, int flag)
{
	DB *db = h;
	int rc, keylen, datlen;
	off_t ptrval;								//rc为状态码

	/* 首先验证flag是否有效，无效则构造core文件退出 */
	if (flag != DB_INSERT && flag != DB_REPLACE && flag != DB_STORE)
	{
		errno = EINVAL;
		return (-1);
	}
	keylen = strlen(key);
	datlen = strlen(data) + 1;
	if (datlen < DATLEN_MIN || datlen > DATLEN_MAX)
		err_dump("db_store: invalid data length");


	if (_db_find_and_lock(db, key, 1) < 0)  /* record not found */
	{
		if (flag == DB_REPLACE)	
		{//修改不存在数据
			rc = -1;
			db->cnt_storerr++;
			errno = ENOENT;   /* error, record does not exist */
			goto doreturn;
		}

		/* _db_find_and_lock locked the hash chain for us;
		read the chain ptr to the first index record on hash chain */
		ptrval = _db_readptr(db, db->chainoff);//读取当前要添加的记录处在的hash链的头指针

		if (_db_findfree(db, keylen, datlen) < 0)//在空链表中查找是否存在键长和数据长相等的键
		{											//插入数据在空间链表中没有匹配键长和数据长，则数据插入文件末尾
			_db_writedat(db, data, 0, SEEK_END); 
			_db_writeidx(db, key, 0, SEEK_END, ptrval);//使得新添加的索引记录中的指针指向未添加这条记录前的hash链的头索引记录

			/*然后再将新记录加到对应的散列链的链首 */
			_db_writeptr(db, db->chainoff, db->idxoff);
			db->cnt_stor1++;//计数器+1
		}
		else
		{//插入数据在空闲链表中匹配到相等键长和数据长，则在将数据插入，并将其从空闲链表删除
			_db_writedat(db, data, db->datoff, SEEK_SET);
			_db_writeidx(db, key, db->idxoff, SEEK_SET, ptrval);  //找记录的工作都在_db_find_free函数里做了，参见此函数
			_db_writeptr(db, db->chainoff, db->idxoff);
			db->cnt_stor2++;
		}
	}
	else //
	{
		if (flag == DB_INSERT)
		{//插入存在数据，退出
			rc = 1;
			db->cnt_storerr++;
			goto doreturn;
		}

		//修改数据，原数据与新数据长度不等
		if (datlen != db->datlen)
		{
			_db_dodelete(db);//删除

			/* Reread the chain ptr in the hash table
			   (it may change with the deletion) */
			ptrval = _db_readptr(db, db->chainoff);

			/* 将新数据添加到索引文件和数据文件的末尾 */
			_db_writedat(db, data, 0, SEEK_END);
			_db_writeidx(db, key, 0, SEEK_END, ptrval);

			/* 将新记录添加到对应散列链的链首 */
			_db_writeptr(db, db->chainoff, db->idxoff);
			db->cnt_stor3++; /* 此种情况的计数器加1 */
		}
		else//新数据与原数据长度相等
		{
			/* 只需重写数据记录，并将计数器加1 */
			_db_writedat(db, data, db->datoff, SEEK_SET);
			db->cnt_stor4++;
		}
	}
	rc = 0;  /* OK */

doreturn:
	//释放锁并退出
	if (un_lock(db->idxfd, db->chainoff, SEEK_SET, 1) < 0)
		err_dump("db_store: unclock error");
	return (rc);
}

/* 从空闲链表中查找是否存在keylen，datlen相等的索引（删除索引只是擦除了key部分） */
static int _db_findfree(DB *db, int keylen, int datlen)
{
	int rc;
	off_t offset, nextoffset, saveoffset;

	//空闲链表加锁
	if (writew_lock(db->idxfd, FREE_OFF, SEEK_SET, 1) < 0)
		err_dump("_db_findfree: writew_lock error");

	//空闲链表头指针
	saveoffset = FREE_OFF;
	offset = _db_readptr(db, saveoffset);

	//遍历链表
	while (offset != 0)
	{
		nextoffset = _db_readidx(db, offset);
		if (strlen(db->idxbuf) == keylen && db->datlen == datlen)
			break;  /* found a match */
		//记录当前位置
		saveoffset = offset;        	//始终保留上一节点的位置，如果找到相应记录，用于修改空闲列表
		offset = nextoffset;
	}

	if (offset == 0)//未匹配到相应索引
	{
		rc = -1;
	}
	else
	{//匹配到相应索引
		//offset表示上一个条索引的位置，ptrval表示下一条索引的位置
		//将上一条索引的next指向ptrval即删除当前节点
		//此时db->idxoff保存当前索引位置
		_db_writeptr(db, saveoffset, db->ptrval);
		rc = 0;
	}

	if (un_lock(db->idxfd, FREE_OFF, SEEK_SET, 1) < 0)
		err_dump("_db_findfree: un_lock error");
	return (rc);  /* 返回状态码 */
}

//将idxoff定位到第一条索引
void db_rewind(DBHANDLE h)
{
	DB *db = h;
	off_t offset;

	offset = (db->nhash + 1) * PTR_SZ;
	if ((db->idxoff = lseek(db->idxfd, offset + 1, SEEK_SET)) == -1)
		err_dump("db_rewind: lseek error");
}

/* 55
	读取数据库的下一行索引，及其数据
	返回db结构的datbuf
*/
char *db_nextrec(DBHANDLE h, char *key)
{
	DB *db = h;
	char c;
	char *ptr;

	if (readw_lock(db->idxfd, FREE_OFF, SEEK_SET, 1) < 0)//给空闲链表加读锁，此时不允许修改空闲链表
		err_dump("db_nextrec: readw_lock error");

	do
	{
		if (_db_readidx(db, 0) < 0) // 读下一条索引，传入0表示从当前位置读 
		{
			ptr = NULL;
			goto doreturn;
		}

		ptr = db->idxbuf;

		/* 可能会读到已删除记录，仅返回有效记录，所以跳过全空格记录 */
		while ((c = *ptr++) != 0 && c == SPACE);
	} while (c == 0);//c==0表示读到空记录，跳过

	if (key != NULL)  /* 找到一个 有效键 */
		strcpy(key, db->idxbuf);

	/* 读数据记录，返回值未指向包含数据记录的内部缓冲的指针值 */
	ptr = _db_readdat(db);
	db->cnt_nextrec++;

doreturn:
	if (un_lock(db->idxfd, FREE_OFF, SEEK_SET, 1) < 0)
		err_dump("db_nextrec: un_lock error");
	return (ptr);
}
