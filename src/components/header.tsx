import { Link } from "./link";

export function Header() {
  return (
    <nav class="navbar navbar-light">
      <div class="container">
        <ul class="nav navbar-nav pull-xs-right">
          <li class="nav-item">
            <Link href="/">Home</Link>
          </li>
          <li class="nav-item">
            <Link href="/exchangerate">Exchangerate</Link>
          </li>
        </ul>
      </div>
    </nav>
  );
}
