package com.pq.bigdata.mr.entity;

import java.io.IOException;
import java.util.Arrays;

import org.apache.commons.lang.StringUtils;

public class Node {
	//成员变量
	private double pageRank = 1.0;
	private String[] adjacentNodeNames;
	//分隔符
	public static final char fieldSeparator = '\t';

	public double getPageRank() {
		return pageRank;
	}

	public Node setPageRank(double pageRank) {
		this.pageRank = pageRank;
		return this;
	}

	public String[] getAdjacentNodeNames() {
		return adjacentNodeNames;
	}

	public Node setAdjacentNodeNames(String[] adjacentNodeNames) {
		this.adjacentNodeNames = adjacentNodeNames;
		return this;
	}

	public boolean containsAdjacentNodes() {
		return adjacentNodeNames != null && adjacentNodeNames.length > 0;
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append(pageRank);

		if (getAdjacentNodeNames() != null) {
			sb.append(fieldSeparator).append(StringUtils.join(getAdjacentNodeNames(), fieldSeparator));
		}
		return sb.toString();
	}

	/**
	 * 
	 * @param value 0.3 B	D
	 * @return
	 * @throws IOException
	 */
	public static Node fromMR(String value) throws IOException {
		//按照分隔符切分数据
		String[] parts = StringUtils.splitPreserveAllTokens(value, fieldSeparator);
		//如果切分后小于一块，说明少了PR值和映射关系
		if (parts.length < 1) {
			throw new IOException("Expected 1 or more parts but received " + parts.length);
		}
		//创建节点对象
		Node node = new Node().setPageRank(Double.valueOf(parts[0]));
		//如果大于1说明有子节点，
		if (parts.length > 1) {
			node.setAdjacentNodeNames(Arrays.copyOfRange(parts, 1, parts.length));
		}
		//返回节点
		return node;
	}

	public static Node fromMR(String v1, String v2) throws IOException {
		return fromMR(v1 + fieldSeparator + v2);
	}
}
