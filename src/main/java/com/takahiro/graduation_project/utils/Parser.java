package com.takahiro.graduation_project.utils;

import java.io.IOException;

public class Parser {
	public static final String[] keywords = {"between", "and", "or", "select", "from", "left", "join", "on", "where",
			"order", "by", "asc", "desc", "group", "having", "table"};
	
	//public static String[] operators = {"=", "<", ">", "<=", ">=", "!=", "(", ")", " "};
	
	public static void main(String[] args) throws IOException{
		
		/*
		StringBuffer stringBuffer = new StringBuffer("and");
		System.out.println(stringBuffer.length());
		if (stringBuffer.toString().equals("and")) {
			System.out.println("true");
		}*/
		
		Parser parser = new Parser();
		String sql1 = new String("A in (1,2) or B=2 and (c in (1,2))");
		sql1 = new String("(a=1 or b=2)");
		sql1 = new String("a=1 or b=2");
		sql1 = new String("a=1 and b=2");
		sql1 = new String("a=1");
		sql1 = new String("a in (1,2,3)");
		sql1 = new String("A=1 or B=2 and (c=3 and d=4)");
		sql1 = new String("A=1 and (b in (1,2)) and c=1 or d=1 and e=1");
		sql1 = new String("(hg00100='0|0' or hg00100='1|0' or hg00100='5|0') "
    			+ "and (hg00245='0|1' or (hg00245='1|1' or hg00245='5|0'))"
    			+ " and (hg00251='0|1' or hg00251='1|1' or hg00251='5|0')");
		//sql1 = new String("a=1 or b=3 and (format in (1,2))");		
		BinaryTree root = parser.parse(sql1, State.NORMAL, null);
		root.postorderTraversal(root);
		System.out.println(Query.judge(root));
		
	}
	
	public boolean check(String sql) {		//关于输入sql的合法性检测，初步打算通过druid的sql检查来实现
		return true;
	}
	

	public static enum State {			//sql状态转换机
		NORMAL, OP_SET_START, OP_SET_END, GT_SET_START, GT_SET_END, IN_SET;
	}
	//public State state = State.NORMAL;
	public boolean bracket = false;
	public StringBuffer readBuffer = new StringBuffer();
	public void refreshBuffer(char c) {
		readBuffer = new StringBuffer();
		readBuffer.append(c);
	}
	public boolean hasOperators(String sql) {	//判断这个sql中是否含有and或者or
		String[] groups = sql.split(" ");
		for (int i = 0; i < groups.length; i++) {
			if (groups[i].equals("and") || groups[i].equals("or")) {
				return true;
			}
		}
		return false;
	}
	
	//递归下降法处理sql语法解析，仔细想一想其实递归下降法的冗余操作非常多，感觉正式的语法解析不是用递归下降的方法
	//突然发现我这个是把词法分析和语法分析糅杂在一起了，怪不得看起来这么别扭，正常的做法应该是分开的才对，后面有时间还是改一改吧
	public BinaryTree parse(String sql,State state, BinaryTree past) {
		BinaryTree leftTree, rightTree, root;
		
		StringBuffer firstOpBuffer = new StringBuffer();
		StringBuffer secondOpBuffer = new StringBuffer();

		StringBuffer leftGtBuffer = new StringBuffer();
		StringBuffer rightGtBuffer = new StringBuffer();
		
		boolean left_bracket_flag = false; 	//标识左数据是否匹配到()这样的内容
		boolean right_bracket_flag = false;	//标识右数据是否匹配到()这样的内容
		
		sql = sql.trim();		//去除左右两个的空格，防止额外的影响
		
		for (int i = 0; i < sql.length(); i++) {
			char c = sql.charAt(i);
			switch (state) {
			case NORMAL:		//相当于一开始的状态
				if (c == '(') {		//相当于匹配左数据段的状态，只是左数据段以()包含的形式
					int flag = 1;
					int index = i+1;
					while (flag != 0) {
						if (sql.charAt(index) == '(') {
							flag++;
						} else if (sql.charAt(index) == ')') {
							flag--;
						}
						index++;
					} 
					index--;	//index表示右括号所在序号
					
					String leftSubSql = sql.substring(i+1, index);		//获得括号里面的内容
					i = index;
					leftGtBuffer = new StringBuffer(leftSubSql);		//将括号里面的内容传入左数据块中
					
					state = State.OP_SET_START;
					left_bracket_flag = true;
					while(i+1 < sql.length() && sql.charAt(i+1) == ' ') {		//跳过() and B的前一个空格
						i++;
					}
				} else if (c == ' ') {
					continue;
				} else {		//相当于匹配左数据段的状态
					state = State.GT_SET_START;
					leftGtBuffer.append(c);
				}
				break;
			case GT_SET_START:		//相当于匹配数据段的状态
				if (c == ' ') {
					if (firstOpBuffer.length() != 0) {//如果数据段匹配完，并且第一个操作符已经设置，说明是A op B模式
						state = State.GT_SET_END;
					} else {
						state = State.OP_SET_START;
					}
					while(i+1 < sql.length() && sql.charAt(i+1) == ' ') {		//跳过多余的空格
						i++;
					}
				} else if (c == '(') {	//相当于匹配右数据段，只不过右数据段以括号()包含的形式
					int flag = 1;
					int index = i+1;
					while (flag != 0) {
						if (sql.charAt(index) == '(') {
							flag++;
						} else if (sql.charAt(index) == ')') {
							flag--;
						}
						index++;
					} 
					index--;	//index表示右括号所在序号
					
					String rightSubSql = sql.substring(i+1, index);		//获得括号里面的内容
					i = index;
					rightGtBuffer = new StringBuffer(rightSubSql);
					state = State.GT_SET_END;
					right_bracket_flag = true;
					while(i+1 < sql.length() && sql.charAt(i+1) == ' ') {		//跳过() and B的前一个空格
						i++;
					}
					
				} else if (firstOpBuffer.length() != 0 || past != null){	//如果past != null或者firstOpBuffer != ''说明左数据段已经匹配
					rightGtBuffer.append(c);
				} else {
					leftGtBuffer.append(c);
				}
				break;
			case GT_SET_END:	//相当于匹配了A and B的形式，然后对A and B形式进行处理
				if (past == null) {
					leftTree = new BinaryTree(firstOpBuffer);
					leftTree.left = parse(leftGtBuffer.toString(), State.NORMAL, null);
					leftTree.right = parse(rightGtBuffer.toString(), State.NORMAL, null);
				} else {
					leftTree = new BinaryTree(firstOpBuffer);
					leftTree.left = past;
					leftTree.right = parse(rightGtBuffer.toString(), State.NORMAL, null);
				}
				root = parse(sql.substring(i), State.OP_SET_START, leftTree);		//这一段好像还是有问题....
				return root;
			case OP_SET_START:	//相当于开始匹配操作符的状态
				if (c == ' ') {
					state = State.OP_SET_END; 
					while(i+1 < sql.length() && sql.charAt(i+1) == ' ') {		//跳过多余的空格
						i++;
					}
				} else {
					firstOpBuffer.append(c);
				}
				break;
			case OP_SET_END:	//相当于匹配完操作符的状态，开始执行下面的操作
				if (firstOpBuffer.toString().equals("and")) {
					state = State.GT_SET_START;
					i--;
				} else if (firstOpBuffer.toString().equals("or")) {
					if (past == null) {
						root = new BinaryTree(firstOpBuffer);
						root.left = parse(leftGtBuffer.toString(), State.NORMAL, null);
						root.right = parse(sql.substring(i), State.NORMAL, null);
					} else {
						root = new BinaryTree(firstOpBuffer);
						root.left = past;
						root.right = parse(sql.substring(i), State.NORMAL, null);
					}
					return root;
				} else if (firstOpBuffer.toString().equals("in")) {
					while(i+1 < sql.length() && sql.charAt(i) == ' ') {		//跳过多余的空格
						i++;
					}
					int flag = 2;
					int start = i,end = i;		//start的位置应该是在a in ()的(上
					while (flag != 0) {
						if (sql.charAt(i) == '(' || sql.charAt(i) == ')') {
							flag--;
						}
						i++;
					}
					end = i;		//这时候i是去到a in () 的后一个字母空格上
					while(i+1 < sql.length() && sql.charAt(i+1) == ' ') {		//跳过多余的空格
						i++;
					}
					state = State.OP_SET_START;
					leftGtBuffer = leftGtBuffer.append(" ").append(firstOpBuffer).append(" ").
							append(sql.substring(start, end));
					firstOpBuffer = new StringBuffer();
				}
				break;
			default:
				break;
			}
		}
		//当后面没有字符的时候，相当于已经遍历完了当前递归层次的sql语句，各种情况如何处理
		if (state == State.GT_SET_START || state == State.OP_SET_START || state == State.GT_SET_END) {
			//有左数据，但是没有op和右数据，说明这个数据是属于整个sql最右的数据
			if (leftGtBuffer.length() != 0 && firstOpBuffer.length() == 0 && rightGtBuffer.length() == 0) {
				if (left_bracket_flag == true) {
					return parse(leftGtBuffer.toString(), State.NORMAL, null);
				} else {
					root = new BinaryTree(leftGtBuffer);
					return root;
				}
			}
			//左数据，op，右数据都有，还要分左右数据是否是括号包含的
			else if (leftGtBuffer.length() != 0 && firstOpBuffer.length() != 0 && rightGtBuffer.length() != 0){
				root = new BinaryTree(firstOpBuffer);
				if (left_bracket_flag == true) {
					root.left = parse(leftGtBuffer.toString(), State.NORMAL, null);
				} else {
					root.left = new BinaryTree(leftGtBuffer);
				}
				if (right_bracket_flag == true) {
					root.right = parse(rightGtBuffer.toString(), State.NORMAL, null);
				} else {
					root.right = new BinaryTree(rightGtBuffer);
				}
				return root;
			} 
			//没有左数据，但是有op和右数据
			else if (leftGtBuffer.length() == 0 && firstOpBuffer.length() != 0 && rightGtBuffer.length() != 0) {
				root = new BinaryTree(firstOpBuffer);
				root.left = past;
				root.right = parse(rightGtBuffer.toString(), State.NORMAL, null);
				return root;
			}
		}
		
		if (past == null) {
			return new BinaryTree();
		} else {
			root = new BinaryTree();
			root.left = past;
			return root;
		}
	}
	
	
	//抽象AST语法树
	class BinaryTree {
		public BinaryTree left;
		public BinaryTree right;
		public BinaryTree parent;
		public StringBuffer data;
		
		public BinaryTree getParent() {
			return parent;
		}
		
		public BinaryTree() {
			
		}
		
		public BinaryTree(StringBuffer data) {
			this.data = data;
			this.left = null;
			this.right = null;
		}
		
		public void addNode(BinaryTree left, BinaryTree right) {
			this.left = left;
			this.right = right;
			left.parent = this;
			right.parent = this;
		}
		
		public void postorderTraversal(BinaryTree root) {
			if (root != null) {
				postorderTraversal(root.left);
				postorderTraversal(root.right);
				if (root.data != null) {
					System.out.println(root.data);
				}
			}
		}
	}
	
	
}

