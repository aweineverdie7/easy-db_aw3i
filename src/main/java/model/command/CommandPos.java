/*
 *@Type CommandPos.java
 * @Desc
 * @Author urmsone urmsone@163.com
 * @date 2024/6/13 02:35
 * @version
 */
package model.command;

import lombok.Getter;
import lombok.Setter;

@Setter
@Getter
public class CommandPos {
    private long pos;
    private int len;
    private String gen;

    public CommandPos(long pos, int len,String gen) {
        this.pos = pos;
        this.len = len;
        this.gen = gen;
    }

    @Override
    public String toString() {
        return "CommandPos{" +
                "pos=" + pos +
                ", len=" + len +
                "gen" +gen+'}';
    }
}
