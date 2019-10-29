#include "util.h"
#include "generator.h"

static const uint64_t kFNVPrime64 = 1099511628211;
unsigned int hashfunc(uint32_t val) {
	uint32_t hash = 123;
	int i;
	for (i = 0; i < sizeof(uint32_t); i++) {
		uint64_t octet = val & 0x00ff;
		val = val >> 8;

		hash = hash ^ octet;
		hash = hash * kFNVPrime64;
	}
	return hash;
}

int ZipfianFunc::randomInt()
{
    double d = rdm.randomDouble();

    int low = 0, high = size;
    while (low < high - 1)
    {
        int mid = (low + high) / 2;
        if (zipfs[mid] <= d && zipfs[mid + 1] > d)
        {
            low = mid; break;
        }
        else if (zipfs[mid] > d)
        {
            high = mid;
        }
        else
        {
            low = mid;
        }
    }
    return hashfunc(low) % size;
}

void ZipfianFunc::init(double s, int inital)
{
    zipfs = new double[inital];
    double sum = 0.0;
    for (int i = 1; i < inital + 1; i++)
    {
        zipfs[i - 1] = 1.0 / (float)pow((double)i, s);
        sum += zipfs[i - 1];
    }
    zipfs[0] = 1.0 / sum;
    for (int i = 1; i < inital; i++)
    {
        zipfs[i] = zipfs[i] / sum + zipfs[i - 1];
    }
}

ZipfianFunc::ZipfianFunc(double s, int inital) : size(inital)
{
    init(s, inital);
}


ZipfGenerator::ZipfGenerator(double s, int initial){
    ZipfianFunc zipf(s, initial);
    for(int i = 0; i < data_size; i++){
        zipfindex[i] = zipf.randomInt() % data_size;
    }
}


