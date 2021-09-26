/**
 * @Author: whzHander
 * @Date: 2021/5/29 13:10
 * @Description:
 */
public class Crossword extends Puzzle implements Solvable{
    Puzzle p = new Puzzle();
    Crossword cw = new Crossword();

    public Crossword getCw() {
        return cw;
    }

    public Puzzle getP() {
        return p;
    }

}
