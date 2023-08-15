package com.github.kjetilv.lopp.files;

import com.github.kjetilv.lopp.Partition;
import com.github.kjetilv.lopp.Transfer;
import com.github.kjetilv.lopp.Transfers;

import java.nio.file.Path;

public final class FileChannelTransfers extends FileChannelBase implements Transfers<Path> {

    public FileChannelTransfers(Path target) {
        super(target, true);
    }

    @Override
    public Transfer transfer(Partition partition, Path result) {
        return new FileChannelTransfer(channel(), partition, result);
    }
}
