// We've received a command to indicate
void processSetLevel(control_t *ptr)
{
	assert(ptr != NULL);
	
	if (BIT_TEST(ptr->mask, LOG_DATA_MASK_LEVEL)) {
		ptr->filter = ptr->level;
	}
}


void processText(control_t *ptr)
{
	assert(ptr != NULL);
	assert(BIT_TEST(ptr->mask, LOG_DATA_MASK_TEXT));
	assert(ptr->text.length > 0);

	printf("LOG: %s\n", expbuf_string(&ptr->text));
	
	// if we dont have a file open, then we will need to open one.
// 	assert(0);
	
	// write the text entry to the file.

	// increment our stats.

	// increment the flag so that the changes can be flushed by the timer.
	
}


