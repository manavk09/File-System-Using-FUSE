/*
 *  Copyright (C) 2023 CS416 Rutgers CS
 *	Tiny File System
 *	File:	rufs.c
 *
 */

#define FUSE_USE_VERSION 26

#include <fuse.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <errno.h>
#include <sys/time.h>
#include <libgen.h>
#include <limits.h>

#include "block.h"
#include "rufs.h"

char diskfile_path[PATH_MAX];

// Declare your in-memory data structures here
struct superblock *superblock;
bitmap_t i_bmap;
bitmap_t d_bmap;
int num_dir = BLOCK_SIZE / sizeof(struct dirent);
/* 
 * Get available inode number from bitmap
 */
int get_avail_ino() {

	// Step 1: Read inode bitmap from disk
	bio_read(superblock->i_bitmap_blk,i_bmap);
	// Step 2: Traverse inode bitmap to find an available slot
	int block = 0;
	while(block < MAX_INUM && get_bitmap(i_bmap,block) != 0){
		block++;
	}
	
	// Step 3: Update inode bitmap and write to disk 
	set_bitmap(i_bmap,block);
	bio_write(superblock->i_bitmap_blk,i_bmap);
	return block;
}

/* 
 * Get available data block number from bitmap
 */
int get_avail_blkno() {

	// Step 1: Read data block bitmap from disk
	bio_read(superblock->d_bitmap_blk,d_bmap);
	// Step 2: Traverse data block bitmap to find an available slot
	int block = 0;
	while(block < MAX_DNUM && get_bitmap(d_bmap,block) != 0){
		block++;
	}

	// Step 3: Update data block bitmap and write to disk 
	set_bitmap(d_bmap,block);
	bio_write(superblock->d_bitmap_blk,d_bmap);
	return block;
}

/* 
 * inode operations
 */
int readi(uint16_t ino, struct inode *inode) {

  	// Step 1: Get the inode's on-disk block number
  	int block = superblock->i_start_blk + ((ino * sizeof(struct inode)) / BLOCK_SIZE);
  	// Step 2: Get offset of the inode in the inode on-disk block
  	int offset = (ino * sizeof(struct inode)) % BLOCK_SIZE;
  	// Step 3: Read the block from disk and then copy into inode structure
  	struct inode *reading_block = malloc(BLOCK_SIZE);
	bio_read(block,(void*)reading_block);
	*inode = reading_block[offset];
	free(reading_block);
	return 0;
}

int writei(uint16_t ino, struct inode *inode) {

	// Step 1: Get the block number where this inode resides on disk
	int block = superblock->i_start_blk + ((ino * sizeof(struct inode)) / BLOCK_SIZE);
	// Step 2: Get the offset in the block where this inode resides on disk
	int offset = (ino * sizeof(struct inode)) % BLOCK_SIZE;
	// Step 3: Write inode to disk 
	struct inode *writing_block = malloc(BLOCK_SIZE);
	bio_read(block,(void*)writing_block);
	writing_block[offset] = *inode;
	bio_write(block,(void*)writing_block);
	free(writing_block);
	return 0;
}


/* 
 * directory operations
 */
int dir_find(uint16_t ino, const char *fname, size_t name_len, struct dirent *dirent) {

	// Step 1: Call readi() to get the inode using ino (inode number of current directory)
	struct inode *curr_dir_inode = malloc(sizeof(struct inode));
	readi(ino,curr_dir_inode);

	// Step 2: Get data block of current directory from inode
	struct dirent *cur_dir_db = malloc(BLOCK_SIZE);

	// Step 3: Read directory's data block and check each directory entry.
	int ptr_index = 0;
	int block_index = 0;
	while(ptr_index < DIRECT_PTR_SIZE){
		if(curr_dir_inode->direct_ptr[ptr_index] != VALID){
			return -1;
		}
		
		//reading in the block
		bio_read(curr_dir_inode->direct_ptr[ptr_index],cur_dir_db);

		//itterate through the block
		while(block_index < num_dir){
			if(cur_dir_db[block_index].valid == VALID && strcmp(fname,cur_dir_db[block_index].name) == 0){
				*dirent = cur_dir_db[block_index];
			}
			block_index++;
		}
		
		block_index = 0;
		ptr_index++;
	}
  //If the name matches, then copy directory entry to dirent structure

	return 0;
}

int dir_add(struct inode dir_inode, uint16_t f_ino, const char *fname, size_t name_len) {

	// Step 1: Read dir_inode's data block and check each directory entry of dir_inode
	struct dirent *cur_dir_db = malloc(BLOCK_SIZE);
	// Step 2: Check if fname (directory name) is already used in other entries
	int ptr_index = 0;
	int block_index = 0;
	while(ptr_index < DIRECT_PTR_SIZE){
		if(dir_inode.direct_ptr[ptr_index] != VALID){
			return -1;
		}
		bio_read(dir_inode.direct_ptr[ptr_index],cur_dir_db);
		while(block_index < num_dir){
			if(cur_dir_db[block_index].valid == VALID && strcmp(fname,cur_dir_db[block_index].name) == 0){
				return -1; // meaning that this name is already in use
			}
		}
		block_index = 0;
		ptr_index++;

	}
	// Step 3: Add directory entry in dir_inode's data block and write to disk
	ptr_index = 0;
	block_index = 0;
	while(ptr_index < DIRECT_PTR_SIZE){
		if(dir_inode.direct_ptr[ptr_index] != VALID){
			//that means no block exists to allocate it
			dir_inode.direct_ptr[ptr_index] = get_avail_blkno();
			

		}
	}
	// Allocate a new data block for this directory if it does not exist

	// Update directory inode

	// Write directory entry

	return 0;
}

int dir_remove(struct inode dir_inode, const char *fname, size_t name_len) {

	// Step 1: Read dir_inode's data block and checks each directory entry of dir_inode
	
	// Step 2: Check if fname exist

	// Step 3: If exist, then remove it from dir_inode's data block and write to disk

	return 0;
}

/* 
 * namei operation
 */
int get_node_by_path(const char *path, uint16_t ino, struct inode *inode) {
	
	// Step 1: Resolve the path name, walk through path, and finally, find its inode.
	// Note: You could either implement it in a iterative way or recursive way
	char *path_arr = strtok((char *)path,"/");
	struct dirent *cur_dir_db = malloc(BLOCK_SIZE);
	cur_dir_db->ino = ino;
	
	while(path_arr != NULL){
		if(dir_find(cur_dir_db->ino,path_arr,strlen(path_arr),cur_dir_db) == -1){
			return -1;
		}
		path_arr = strtok(NULL,"/");
	}

	readi(cur_dir_db->ino,inode);
	return 0;
}

/* 
 * Make file system
 */
int rufs_mkfs() {

	// Call dev_init() to initialize (Create) Diskfile
	dev_init(diskfile_path);
	// write superblock information
	superblock = malloc(sizeof(struct superblock));
	superblock->i_bitmap_blk = 1;
	superblock->d_bitmap_blk = 2;
	superblock->i_start_blk = 3;
	superblock->d_start_blk = 3 + ((sizeof(struct inode) * MAX_INUM) / BLOCK_SIZE); //67
	superblock->magic_num = MAGIC_NUM;
	superblock->max_dnum = MAX_DNUM;
	superblock->max_inum = MAX_INUM;
	// initialize inode bitmap
	i_bmap = malloc(BLOCK_SIZE);
	// initialize data block bitmap
	d_bmap = malloc(BLOCK_SIZE);
	// update bitmap information for root directory
	set_bitmap(d_bmap,0); //super block
	set_bitmap(d_bmap,1); //i_bmap
	set_bitmap(d_bmap,2); //d_map
	//setting the inodes
	int index = superblock->i_start_blk;
	while(index < superblock->d_start_blk){
		set_bitmap(d_bmap,index);
		index++;
	}
	bio_write(superblock->d_bitmap_blk,d_bmap);
	// update inode for root directory
	struct inode *root_dir_inode = malloc(BLOCK_SIZE);
	bio_read(superblock->i_start_blk,root_dir_inode);
	root_dir_inode->ino = get_avail_ino(); //inode 0
	printf("inode num: %d\n", root_dir_inode->ino);
	root_dir_inode->valid = 1; //is valid
	root_dir_inode->type = 1; //dir
	memset(root_dir_inode->direct_ptr,0,sizeof(int)*16);
	root_dir_inode->direct_ptr[0] = get_avail_blkno();//block 67
	printf("blk num: %d\n", root_dir_inode->direct_ptr[0]);
	//stat stuff later
	bio_write(superblock->i_start_blk,root_dir_inode);
	//creating the parent and root dirent 
	struct dirent *root_dir = malloc(BLOCK_SIZE);
	root_dir[0].ino = 0;
	root_dir[0].valid = 1;
	strcpy(root_dir[0].name,".");
	root_dir[0].len = strlen(root_dir[0].name);
	//parent
	root_dir[1].ino = 0;
	root_dir[1].valid = 1;
	strcpy(root_dir[1].name,"..");
	root_dir[1].len = strlen(root_dir[1].name);
	bio_write(superblock->d_start_blk, root_dir); //67
	printf("finished\n");
	return 0;
}


/* 
 * FUSE file operations
 */
static void *rufs_init(struct fuse_conn_info *conn) {

	// Step 1a: If disk file is not found, call mkfs
	if(dev_open(diskfile_path) == -1){
		rufs_mkfs();
	}
	
  // Step 1b: If disk file is found, just initialize in-memory data structures
  // and read superblock from disk
  superblock = malloc(BLOCK_SIZE);
  bio_read(0,superblock);
  d_bmap = malloc(BLOCK_SIZE);
  i_bmap = malloc(BLOCK_SIZE);
  bio_read(superblock->d_bitmap_blk,d_bmap);
  bio_read(superblock->i_bitmap_blk, i_bmap);

	return NULL;
}

static void rufs_destroy(void *userdata) {

	// Step 1: De-allocate in-memory data structures
	free(superblock); //free stat before do that after adding stat stuff
	free(d_bmap);
	free(i_bmap);
	// Step 2: Close diskfile
	dev_close();
}

static int rufs_getattr(const char *path, struct stat *stbuf) {

	// Step 1: call get_node_by_path() to get inode from path

	// Step 2: fill attribute of file into stbuf from inode

		stbuf->st_mode   = S_IFDIR | 0755;
		stbuf->st_nlink  = 2;
		time(&stbuf->st_mtime);

	return 0;
}

static int rufs_opendir(const char *path, struct fuse_file_info *fi) {

	// Step 1: Call get_node_by_path() to get inode from path

	// Step 2: If not find, return -1

    return 0;
}

static int rufs_readdir(const char *path, void *buffer, fuse_fill_dir_t filler, off_t offset, struct fuse_file_info *fi) {

	// Step 1: Call get_node_by_path() to get inode from path

	// Step 2: Read directory entries from its data blocks, and copy them to filler

	return 0;
}


static int rufs_mkdir(const char *path, mode_t mode) {

	// Step 1: Use dirname() and basename() to separate parent directory path and target directory name

	// Step 2: Call get_node_by_path() to get inode of parent directory

	// Step 3: Call get_avail_ino() to get an available inode number

	// Step 4: Call dir_add() to add directory entry of target directory to parent directory

	// Step 5: Update inode for target directory

	// Step 6: Call writei() to write inode to disk
	

	return 0;
}

static int rufs_rmdir(const char *path) {

	// Step 1: Use dirname() and basename() to separate parent directory path and target directory name

	// Step 2: Call get_node_by_path() to get inode of target directory

	// Step 3: Clear data block bitmap of target directory

	// Step 4: Clear inode bitmap and its data block

	// Step 5: Call get_node_by_path() to get inode of parent directory

	// Step 6: Call dir_remove() to remove directory entry of target directory in its parent directory

	return 0;
}

static int rufs_releasedir(const char *path, struct fuse_file_info *fi) {
	// For this project, you don't need to fill this function
	// But DO NOT DELETE IT!
    return 0;
}

static int rufs_create(const char *path, mode_t mode, struct fuse_file_info *fi) {

	// Step 1: Use dirname() and basename() to separate parent directory path and target file name

	// Step 2: Call get_node_by_path() to get inode of parent directory

	// Step 3: Call get_avail_ino() to get an available inode number

	// Step 4: Call dir_add() to add directory entry of target file to parent directory

	// Step 5: Update inode for target file

	// Step 6: Call writei() to write inode to disk

	return 0;
}

static int rufs_open(const char *path, struct fuse_file_info *fi) {

	// Step 1: Call get_node_by_path() to get inode from path

	// Step 2: If not find, return -1

	return 0;
}

static int rufs_read(const char *path, char *buffer, size_t size, off_t offset, struct fuse_file_info *fi) {

	// Step 1: You could call get_node_by_path() to get inode from path

	// Step 2: Based on size and offset, read its data blocks from disk

	// Step 3: copy the correct amount of data from offset to buffer

	// Note: this function should return the amount of bytes you copied to buffer
	return 0;
}

static int rufs_write(const char *path, const char *buffer, size_t size, off_t offset, struct fuse_file_info *fi) {
	// Step 1: You could call get_node_by_path() to get inode from path

	// Step 2: Based on size and offset, read its data blocks from disk

	// Step 3: Write the correct amount of data from offset to disk

	// Step 4: Update the inode info and write it to disk

	// Note: this function should return the amount of bytes you write to disk
	return size;
}

static int rufs_unlink(const char *path) {

	// Step 1: Use dirname() and basename() to separate parent directory path and target file name

	// Step 2: Call get_node_by_path() to get inode of target file

	// Step 3: Clear data block bitmap of target file

	// Step 4: Clear inode bitmap and its data block

	// Step 5: Call get_node_by_path() to get inode of parent directory

	// Step 6: Call dir_remove() to remove directory entry of target file in its parent directory

	return 0;
}

static int rufs_truncate(const char *path, off_t size) {
	// For this project, you don't need to fill this function
	// But DO NOT DELETE IT!
    return 0;
}

static int rufs_release(const char *path, struct fuse_file_info *fi) {
	// For this project, you don't need to fill this function
	// But DO NOT DELETE IT!
	return 0;
}

static int rufs_flush(const char * path, struct fuse_file_info * fi) {
	// For this project, you don't need to fill this function
	// But DO NOT DELETE IT!
    return 0;
}

static int rufs_utimens(const char *path, const struct timespec tv[2]) {
	// For this project, you don't need to fill this function
	// But DO NOT DELETE IT!
    return 0;
}


static struct fuse_operations rufs_ope = {
	.init		= rufs_init,
	.destroy	= rufs_destroy,

	.getattr	= rufs_getattr,
	.readdir	= rufs_readdir,
	.opendir	= rufs_opendir,
	.releasedir	= rufs_releasedir,
	.mkdir		= rufs_mkdir,
	.rmdir		= rufs_rmdir,

	.create		= rufs_create,
	.open		= rufs_open,
	.read 		= rufs_read,
	.write		= rufs_write,
	.unlink		= rufs_unlink,

	.truncate   = rufs_truncate,
	.flush      = rufs_flush,
	.utimens    = rufs_utimens,
	.release	= rufs_release
};


int main(int argc, char *argv[]) {
	int fuse_stat;

	getcwd(diskfile_path, PATH_MAX);
	strcat(diskfile_path, "/DISKFILE");

	fuse_stat = fuse_main(argc, argv, &rufs_ope, NULL);

	return fuse_stat;
}

